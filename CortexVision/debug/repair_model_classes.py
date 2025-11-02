"""Repair helper to reconstruct AI_MODELS and MODEL_CLASSES from IMAGE_METADATA.

This script inserts missing models and model->class mappings derived from
the IMAGE_METADATA.FILE_PATH (which includes model and class directories) and
IMAGE_METADATA.CAPTION (the readable class name).

It avoids deleting existing rows and only inserts missing mappings.
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
        print("Connecting to Snowflake...")
        csf.connect()

        print("Inserting missing models from IMAGE_METADATA into AI_MODELS...")
        # Extract model from file path: parts are like '@.../Model/Class/file'
        insert_models_sql = """
        INSERT INTO VISIONDB.HACKATHON_SCHEMA.AI_MODELS (MODEL_NAME)
        SELECT DISTINCT model FROM (
            SELECT SPLIT_PART(FILE_PATH, '/', 2) AS model FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA
        ) WHERE model IS NOT NULL
        AND model NOT IN (SELECT MODEL_NAME FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS);
        """
        run(csf, insert_models_sql, fetch=False)

        print("Inserting missing model->class mappings into MODEL_CLASSES (from IMAGE_METADATA.CAPTION)...")
        insert_classes_sql = """
        INSERT INTO VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES (MODEL_NAME, CLASS_NAME)
        SELECT DISTINCT model, caption FROM (
            SELECT SPLIT_PART(FILE_PATH, '/', 2) AS model, CAPTION as caption
            FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA
            WHERE CAPTION IS NOT NULL
        ) src
        WHERE (model, caption) NOT IN (
            SELECT MODEL_NAME, CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES
        );
        """
        run(csf, insert_classes_sql, fetch=False)

        print("Repair operations completed. Verify via admin or debug scripts.")

    finally:
        try:
            csf.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
