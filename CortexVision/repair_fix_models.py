"""Clean up AI_MODELS and MODEL_CLASSES inserted with filename-like model names.

This script removes obviously-bad model entries (those that look like filenames ending with .jpg)
and inserts the correct model/class mappings derived from IMAGE_METADATA where the model
path segment doesn't look like a filename.
"""
import traceback
from snowflake_conn import CustomSnowflake


def run(csf, sql, fetch=True):
    try:
        rows, rc = csf.run_command(sql, fetch=fetch)
        return rows
    except Exception as e:
        print(f"Query failed: {e}")
        traceback.print_exc()
        return None


def main():
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()

        print("Removing AI_MODELS entries that look like filenames (ending with .jpg)...")
        run(csf, "DELETE FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS WHERE LOWER(MODEL_NAME) LIKE '%.jpg'", fetch=False)

        print("Inserting cleaned model names from IMAGE_METADATA (ignoring filename-like segments)...")
        run(csf, "INSERT INTO VISIONDB.HACKATHON_SCHEMA.AI_MODELS (MODEL_NAME) SELECT DISTINCT SPLIT_PART(FILE_PATH, '/', 2) AS model FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE SPLIT_PART(FILE_PATH, '/', 2) IS NOT NULL AND LOWER(SPLIT_PART(FILE_PATH, '/', 2)) NOT LIKE '%.jpg' AND SPLIT_PART(FILE_PATH, '/', 2) NOT IN (SELECT MODEL_NAME FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS)", fetch=False)

        print("Removing MODEL_CLASSES rows where model looks like filename or class looks like filename...")
        run(csf, "DELETE FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES WHERE LOWER(MODEL_NAME) LIKE '%.jpg' OR LOWER(CLASS_NAME) LIKE '%.jpg'", fetch=False)

        print("Inserting cleaned MODEL_CLASSES (MODEL_NAME from FILE_PATH part2, CLASS_NAME from CAPTION)...")
        run(csf, "INSERT INTO VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES (MODEL_NAME, CLASS_NAME) SELECT DISTINCT SPLIT_PART(FILE_PATH, '/', 2) AS model, CAPTION FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE SPLIT_PART(FILE_PATH, '/', 2) IS NOT NULL AND LOWER(SPLIT_PART(FILE_PATH, '/', 2)) NOT LIKE '%.jpg' AND CAPTION IS NOT NULL AND (SPLIT_PART(FILE_PATH, '/', 2), CAPTION) NOT IN (SELECT MODEL_NAME, CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES)", fetch=False)

        print("Cleanup complete. Verify results with debug_db.py or via admin UI.")

    finally:
        try:
            csf.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
