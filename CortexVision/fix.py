import os
from snowflake_conn import CustomSnowflake
import sys

# The model we use to generate image embeddings
EMBEDDING_MODEL = 'snowflake-arctic-embed-m'

def backfill_image_vectors():
    """
    Connects to Snowflake and populates the IMAGE_VECTOR column for any
    rows where it is currently empty.
    """
    print("Connecting to Snowflake...")
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        print("Connection successful.")

        # Find all images that are missing a vector embedding
        sql_select = "SELECT FILE_PATH FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE IMAGE_VECTOR IS NULL"
        rows, _ = csf.run_command(sql_select, fetch=True)

        if not rows:
            print("All images already have vector embeddings. Nothing to do.")
            return

        print(f"Found {len(rows)} images to process. This may take a few minutes...")

        for i, row in enumerate(rows):
            # Using single quotes for file paths can be tricky if they contain quotes.
            # Using bind variables is safer.
            staged_file_path = row[0]
            print(f"Processing ({i+1}/{len(rows)}): {staged_file_path}")

            # Construct the SQL to ask Cortex to create an embedding and update the row
            sql_update = f"""
            UPDATE VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA
            SET IMAGE_VECTOR = SNOWFLAKE.CORTEX.EMBED_IMAGE('{EMBEDDING_MODEL}', '{staged_file_path}')
            WHERE FILE_PATH = '{staged_file_path}' AND IMAGE_VECTOR IS NULL
            """
            try:
                # We don't need to fetch results for an UPDATE command
                csf.run_command(sql_update, fetch=False)
            except Exception as e:
                print(f"  -> Failed to process {staged_file_path}: {e}", file=sys.stderr)

        print("\nBackfill complete!")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
    finally:
        try:
            csf.close()
            print("Connection closed.")
        except:
            pass

if __name__ == "__main__":
    backfill_image_vectors()