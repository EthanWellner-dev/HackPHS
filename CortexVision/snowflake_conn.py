import os
import logging
from typing import Any, Iterable, Optional, Tuple

from dotenv import load_dotenv
import snowflake.connector
import hashlib

# Load environment variables from a local .env file (if present) and system env
load_dotenv()

logger = logging.getLogger(__name__)
if not logger.handlers:
    # Basic configuration if the application hasn't configured logging yet
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

class CustomSnowflake:
    """Helper class to manage a Snowflake connection and run commands.

    Features:
    - Initialize with explicit connection parameters or pick them up from env vars
    - connect() and close() to manage the connection lifecycle
    - put_file(local_path, stage_target) to run the Snowflake PUT command
    - run_command(sql, params) to execute arbitrary SQL/commands

    Usage:
        csf = CustomSnowflake.from_env()
        csf.connect()
        csf.put_file(r"C:\path\to\file.csv", "@~")
        rows = csf.run_command("SELECT CURRENT_VERSION()")
        csf.close()
    """

    def __init__(
        self,
        user: Optional[str] = None,
        password: Optional[str] = None,
        account: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        client_session_keep_alive: bool = False,
    ) -> None:
        # Use provided args, falling back to environment variables
        self.conn_kwargs = {
            "user": user or os.getenv("SNOWFLAKE_USER"),
            "password": password or os.getenv("SNOWFLAKE_PASSWORD"),
            "account": account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": warehouse or os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": database or os.getenv("SNOWFLAKE_DATABASE"),
            "schema": schema or os.getenv("SNOWFLAKE_SCHEMA"),
        }
        if role:
            self.conn_kwargs["role"] = role
        # Optional Snowflake-specific args
        if client_session_keep_alive:
            self.conn_kwargs["client_session_keep_alive"] = True

        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    @classmethod
    def from_env(cls) -> "CustomSnowflake":
        """Create an instance using environment variables."""
        return cls()

    def connect(self) -> None:
        """Open a Snowflake connection using stored connection kwargs.

        Raises an exception if connection fails.
        """
        if self._conn is not None:
            logger.debug("Connection already open")
            return

        # Remove None values to avoid passing them to connector
        conn_args = {k: v for k, v in self.conn_kwargs.items() if v is not None}
        logger.info("Connecting to Snowflake (account=%s, user=%s)", conn_args.get("account"), conn_args.get("user"))
        try:
            self._conn = snowflake.connector.connect(**conn_args)
            logger.info("Connected to Snowflake")
        except Exception:
            logger.exception("Failed to connect to Snowflake")
            raise

    def close(self) -> None:
        """Close the Snowflake connection if open."""
        if self._conn:
            try:
                self._conn.close()
                logger.info("Snowflake connection closed")
            except Exception:
                logger.exception("Error while closing Snowflake connection")
            finally:
                self._conn = None

    def _ensure_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None:
            raise RuntimeError("Snowflake connection is not open. Call connect() first.")
        return self._conn

    def put_file(self, local_path: str, stage_target: str = "@~", parallel: Optional[int] = None) -> dict:
        """Run a Snowflake PUT command to upload a local file or all top-level files in a directory to a stage.

        If local_path is a directory, uploads every file directly inside that directory (no recursion).
        Returns aggregated results across all PUT operations.
        """
        conn = self._ensure_conn()

        # Helper to normalize a filesystem path to a Snowflake file:// URL
        def _to_file_url(path: str) -> str:
            abs_path = os.path.abspath(path)
            url_path = abs_path.replace("\\", "/")
            return f"file:///{url_path}" if not url_path.startswith("file://") else url_path

        # Determine files to put: single file or all files in directory (non-recursive)
        if os.path.isdir(local_path):
            abs_dir = os.path.abspath(local_path)
            files = [
                os.path.join(abs_dir, fname)
                for fname in os.listdir(abs_dir)
                if os.path.isfile(os.path.join(abs_dir, fname))
            ]
            if not files:
                raise ValueError(f"No files found in directory: {local_path}")
        else:
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Path does not exist: {local_path}")
            files = [local_path]

        aggregated_rows = []
        total_rowcount = 0
        last_description = None

        logger.info("Running PUT for %d file(s) to %s", len(files), stage_target)
        cur = conn.cursor()
        try:
            for fpath in files:
                file_url = _to_file_url(fpath)
                put_sql = f"PUT '{file_url}' {stage_target} AUTO_COMPRESS=FALSE"
                if parallel is not None:
                    put_sql += f" PARALLEL={int(parallel)}"
                logger.info("Running PUT command: %s", put_sql)
                cur.execute(put_sql)
                # PUT typically returns a result set describing uploaded files
                try:
                    rows = cur.fetchall()
                except Exception:
                    rows = []
                if rows:
                    aggregated_rows.extend(rows)
                # cur.rowcount may be -1 for some drivers; sum what makes sense
                try:
                    if isinstance(cur.rowcount, int) and cur.rowcount >= 0:
                        total_rowcount += cur.rowcount
                except Exception:
                    pass
                last_description = cur.description
                logger.info("PUT for %s completed, rows=%d", fpath, len(rows))
            return {"rows": aggregated_rows, "rowcount": total_rowcount, "description": last_description, "uploaded_files": files}
        except Exception:
            logger.exception("PUT command(s) failed")
            raise
        finally:
            cur.close()

    def insert_image_metadata_from_local_dir(self, local_path: str, stage_target: str, caption: Optional[str] = None) -> int:
        """Insert metadata rows for all top-level files in local_path into IMAGE_METADATA.

        Creates rows: (IMAGE_ID, FILE_PATH, CAPTION) where:
         - IMAGE_ID = filename without extension
         - FILE_PATH = f"{stage_target}/{basename}"
         - CAPTION = provided caption or IMAGE_ID

        Returns number of attempted inserts.
        """
        conn = self._ensure_conn()

        # collect top-level files (non-recursive)
        if not os.path.isdir(local_path):
            raise ValueError(f"Expected a directory for metadata insertion: {local_path}")
        abs_dir = os.path.abspath(local_path)
        files = [
            os.path.join(abs_dir, fname)
            for fname in os.listdir(abs_dir)
            if os.path.isfile(os.path.join(abs_dir, fname))
        ]
        if not files:
            return 0

        rows = []
        for path in files:
            basename = os.path.basename(path)
            image_id = os.path.splitext(basename)[0]
            stage_file = f"{stage_target}/{basename}"
            # compute sha256 hash of file content to enable exact-match by content
            try:
                with open(path, 'rb') as fh:
                    file_hash = hashlib.sha256(fh.read()).hexdigest()
            except Exception:
                file_hash = None
            rows.append((image_id, stage_file, caption if caption is not None else image_id, file_hash))

        cur = conn.cursor()
        # Try to ensure a FILE_HASH column exists; if this fails (permissions or already exists), ignore
        try:
            try:
                self.run_command("ALTER TABLE VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA ADD COLUMN FILE_HASH VARCHAR", fetch=False)
            except Exception:
                # ignore errors from alter (e.g., column exists or insufficient privileges)
                pass

            insert_sql_with_hash = (
                "INSERT INTO VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA "
                "(IMAGE_ID, FILE_PATH, CAPTION, FILE_HASH) VALUES (%s, %s, %s, %s)"
            )
            # Try to insert with file_hash column; if that fails, fall back to older schema
            for params in rows:
                try:
                    cur.execute(insert_sql_with_hash, params)
                except Exception:
                    # attempt fallback to 3-column insert for this row
                    basename = params[1]
                    image_id = params[0]
                    caption_val = params[2]
                    try:
                        cur.execute(
                            "INSERT INTO VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA (IMAGE_ID, FILE_PATH, CAPTION) VALUES (%s, %s, %s)",
                            (image_id, basename, caption_val),
                        )
                    except Exception:
                        # if even the fallback fails for a row, re-raise to be visible
                        raise

            try:
                conn.commit()
            except Exception:
                logger.debug("Commit failed or unnecessary after metadata insert")

            return len(rows)
        except Exception:
            logger.exception("Failed to insert image metadata")
            raise
        finally:
            cur.close()


    def add_class_embedding(self, class_id: str, class_name: str) -> None:
        """Checks if a class exists and, if not, inserts its vector embedding.

        This is the "AI training" step.
        """
        conn = self._ensure_conn()
        
        # First, check if this class name already exists to avoid duplicates
        check_sql = "SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS WHERE CLASS_NAME = %s"
        rows, _ = self.run_command(check_sql, params=(class_name,))
        if rows and rows[0][0] > 0:
            logger.warning("Class '%s' already has an embedding. Skipping.", class_name)
            return

        logger.info("Creating new class embedding for '%s'", class_name)
        # Note: Using f-string for the function call itself is standard for Cortex
        # We are still using parameterized queries for the user-provided class_name where it's inserted as data.
        insert_sql = f"""
        INSERT INTO VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS (CLASS_ID, CLASS_NAME, TEXT_VECTOR)
        SELECT %s, %s, SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', %s);
        """


        try:
            # We run this as a command that doesn't fetch results
            self.run_command(insert_sql, params=(class_id, class_name, class_name), fetch=False)
            logger.info("Successfully created embedding for '%s'.", class_name)
        except Exception:
            logger.exception("Failed to create class embedding for '%s'", class_name)
            raise
    def get_next_class_id(self) -> str:
        """Calculates the next class ID (e.g., 'c5') by finding the max numeric ID in the database.

        Handles the case where the table is empty, starting with 'c1'.
        """
        # This SQL is designed to be robust. It finds the highest number from IDs like 'c1', 'c10', 'c2'.
        # 1. It filters for IDs that start with 'c'.
        # 2. It removes the 'c' prefix.
        # 3. It casts the remaining number string to an INTEGER to find the true maximum.
        # 4. NVL handles the case where the table is empty, returning 0 instead of NULL.
        sql = """
        SELECT NVL(MAX(CAST(REPLACE(CLASS_ID, 'c', '') AS INTEGER)), 0)
        FROM VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS
        WHERE STARTSWITH(CLASS_ID, 'c');
        """
        rows, _ = self.run_command(sql, fetch=True)

        if not rows:
            # This is a fallback in case the query returns nothing, which is unlikely with NVL.
            max_id_num = 0
        else:
            max_id_num = rows[0][0]

        next_id_num = max_id_num + 1
        next_class_id = f"c{next_id_num}"
        
        logger.info(f"Determined next available class ID: {next_class_id}")
        return next_class_id

    def run_command(self, sql: str, params: Optional[Iterable[Any]] = None, fetch: bool = True) -> Tuple[Optional[Iterable[Tuple[Any, ...]]], int]:
        """Execute an arbitrary SQL/command against Snowflake.

        Args:
            sql: SQL string or Snowflake command to execute.
            params: Optional iterable of parameters to pass to execute().
            fetch: If True and the statement returns rows, fetch and return them.

        Returns:
            A tuple (rows_or_none, rowcount). If no rows are returned, rows_or_none is None.
        """
        conn = self._ensure_conn()
        logger.info("Executing SQL: %s", sql if len(sql) < 200 else sql[:200] + "...")
        cur = conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            if fetch and cur.description:
                rows = cur.fetchall()
                logger.info("Query returned %d rows", len(rows))
                return rows, cur.rowcount
            else:
                logger.info("Command affected %s rows", cur.rowcount)
                return None, cur.rowcount
        except Exception:
            logger.exception("Failed to execute SQL/command")
            raise
        finally:
            cur.close()

    # --- Model management helpers ---
    def ensure_model_tables(self) -> None:
        """Create AI helper tables if they do not exist.

        Tables created:
        - AI_MODELS(MODEL_NAME VARCHAR)
        - MODEL_CLASSES(MODEL_NAME VARCHAR, CLASS_NAME VARCHAR)
        - EMBED_MODELS(MODEL_NAME VARCHAR)
        """
        try:
            create_models = (
                "CREATE TABLE IF NOT EXISTS VISIONDB.HACKATHON_SCHEMA.AI_MODELS (MODEL_NAME VARCHAR)"
            )
            create_classes = (
                "CREATE TABLE IF NOT EXISTS VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES (MODEL_NAME VARCHAR, CLASS_NAME VARCHAR)"
            )
            create_embed = (
                "CREATE TABLE IF NOT EXISTS VISIONDB.HACKATHON_SCHEMA.EMBED_MODELS (MODEL_NAME VARCHAR)"
            )
            # fire-and-forget: we don't fetch results
            self.run_command(create_models, fetch=False)
            self.run_command(create_classes, fetch=False)
            self.run_command(create_embed, fetch=False)
            # ensure there is at least one embed model
            rows, _ = self.run_command("SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.EMBED_MODELS", fetch=True)
            if rows and rows[0][0] == 0:
                # Insert a default embedding model used elsewhere
                try:
                    self.run_command("INSERT INTO VISIONDB.HACKATHON_SCHEMA.EMBED_MODELS (MODEL_NAME) VALUES (%s)", params=("snowflake-arctic-embed-m",), fetch=False)
                except Exception:
                    # ignore insertion errors (permissions)
                    pass
        except Exception:
            # if any of the above fails (permissions, missing DB), just log and continue
            logger.debug("ensure_model_tables: creation/check failed")

    def get_models(self) -> list:
        """Return a list of AI model names (strings) from AI_MODELS table.

        If DB access fails, returns an empty list.
        """
        try:
            rows, _ = self.run_command("SELECT MODEL_NAME FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS ORDER BY MODEL_NAME", fetch=True)
            if rows:
                return [r[0] for r in rows]
        except Exception:
            logger.debug("get_models failed")
        return []

    def add_model(self, model_name: str) -> None:
        """Add a model name to AI_MODELS table (no-op if already present).

        If DB access fails, this will raise the underlying exception.
        """
        # create tables if necessary
        try:
            self.ensure_model_tables()
            # check exists
            rows, _ = self.run_command("SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS WHERE MODEL_NAME = %s", params=(model_name,), fetch=True)
            if not rows or rows[0][0] == 0:
                self.run_command("INSERT INTO VISIONDB.HACKATHON_SCHEMA.AI_MODELS (MODEL_NAME) VALUES (%s)", params=(model_name,), fetch=False)
        except Exception:
            logger.exception("add_model failed")
            raise

    def get_classes_for_model(self, model_name: str) -> list:
        """Return list of class names registered for a model from MODEL_CLASSES table."""
        try:
            rows, _ = self.run_command("SELECT CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES WHERE MODEL_NAME = %s ORDER BY CLASS_NAME", params=(model_name,), fetch=True)
            if rows:
                return [r[0] for r in rows]
        except Exception:
            logger.debug("get_classes_for_model failed for %s", model_name)
        return []

    def add_class_to_model(self, model_name: str, class_name: str) -> None:
        """Insert mapping (model_name, class_name) into MODEL_CLASSES if not exists."""
        try:
            self.ensure_model_tables()
            rows, _ = self.run_command("SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES WHERE MODEL_NAME = %s AND CLASS_NAME = %s", params=(model_name, class_name), fetch=True)
            if not rows or rows[0][0] == 0:
                self.run_command("INSERT INTO VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES (MODEL_NAME, CLASS_NAME) VALUES (%s, %s)", params=(model_name, class_name), fetch=False)
        except Exception:
            logger.exception("add_class_to_model failed")
            raise

    def get_embed_models(self) -> list:
        """Return configured embed model names from EMBED_MODELS table, or fallback to a small local list."""
        try:
            rows, _ = self.run_command("SELECT MODEL_NAME FROM VISIONDB.HACKATHON_SCHEMA.EMBED_MODELS ORDER BY MODEL_NAME", fetch=True)
            if rows:
                return [r[0] for r in rows]
        except Exception:
            logger.debug("get_embed_models failed; returning defaults")
        # fallback defaults
        return ["snowflake-arctic-embed-m", "openai-embedding-ada-002"]


# REPLACE YOUR OLD if __name__ == "__main__": BLOCK WITH THIS ONE

if __name__ == "__main__":
    # --- Configuration: This is the only section you need to change ---
    # The local folder containing the images for ONE new class.
    IMAGE_DIR_PATH = r"C:\Users\eboyw\OneDrive\DEV\Git Clones\ethan-se-25-26\HackPHS\CortexVision\Orange Water Bottle"
    
    # The name of the class. This will be used as the caption and for the AI embedding.
    CLASS_NAME = "Orange Water Bottle"
    
    # The full name of your image stage.
    STAGE_NAME = "@VISIONDB.HACKATHON_SCHEMA.IMAGE_STAGE"
    # --- End of Configuration ---

    csf = CustomSnowflake.from_env()
    try:
        csf.connect()

        # --- Automated Workflow ---
        # First, determine the next available CLASS_ID automatically.
        logger.info("--- Step 0: Determining Next Class ID ---")
        class_id = csf.get_next_class_id()

        # Step 1: Upload all images from the directory to the Snowflake stage.
        logger.info("\n--- Step 1: Uploading Images ---")
        upload_result = csf.put_file(IMAGE_DIR_PATH, STAGE_NAME)
        logger.info("Uploaded %d files.", len(upload_result.get("uploaded_files", [])))

        # Step 2: Insert the metadata for all the uploaded images.
        logger.info("\n--- Step 2: Inserting Metadata ---")
        inserted_rows = csf.insert_image_metadata_from_local_dir(IMAGE_DIR_PATH, STAGE_NAME, caption=CLASS_NAME)
        logger.info("Inserted metadata for %d images.", inserted_rows)

        # Step 3: "Train" the AI by adding the new class embedding with the generated ID.
        logger.info("\n--- Step 3: Creating AI Embedding ---")
        csf.add_class_embedding(class_id=class_id, class_name=CLASS_NAME)
        
        # Step 4: Commit the transaction to save all changes.
        logger.info("\n--- Step 4: Committing Transaction ---")
        csf._conn.commit()
        logger.info("All changes committed successfully!")

    except Exception as e:
        logger.error("An error occurred during the ingestion process: %s", e)
        # If something fails, roll back any partial changes
        if csf._conn:
            csf._conn.rollback()
            logger.info("Transaction rolled back.")
    finally:
        csf.close()