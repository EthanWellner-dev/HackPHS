"""Debug helper to inspect Vision DB tables and optionally run a test classification.

Usage:
  - Ensure your Snowflake env vars are set (or .env exists).
  - Optionally set TEST_IMAGE and IMAGE_STAGE environment vars to run a single classification.

This script prints counts and up to 5 sample rows from key tables used by the app.
"""
import os
import traceback
from snowflake_conn import CustomSnowflake


def run_query(csf, sql, fetch=True):
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
        print("Connecting to Snowflake using env vars...")
        csf.connect()

        targets = {
            'CLASS_EMBEDDINGS': 'SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS',
            'IMAGE_METADATA': 'SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA',
            'MODEL_CLASSES': 'SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES',
            'AI_MODELS': 'SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS',
        }

        for name, q in targets.items():
            rows = run_query(csf, q)
            print(f"{name} ->", rows[0][0] if rows else 'error/no data')

        print('\nSample rows (up to 5)')
        samples = {
            # Avoid casting vector columns or referencing CREATED_AT which may not exist
            'CLASS_EMBEDDINGS': 'SELECT CLASS_ID, CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS LIMIT 5',
            'IMAGE_METADATA': 'SELECT IMAGE_ID, FILE_PATH, CAPTION FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA LIMIT 5',
            'MODEL_CLASSES': 'SELECT MODEL_NAME, CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES LIMIT 5',
            'AI_MODELS': 'SELECT MODEL_NAME FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS LIMIT 5',
        }

        for name, q in samples.items():
            rows = run_query(csf, q)
            print(f"\n-- {name} sample --")
            if not rows:
                print("(no rows or query failed)")
                continue
            for r in rows:
                print(r)

        # Optional quick classification test if env vars provided
        test_image = os.environ.get('TEST_IMAGE')
        test_stage = os.environ.get('IMAGE_STAGE') or os.environ.get('STAGE')
        test_model = os.environ.get('TEST_MODEL')
        if test_image and test_stage:
            print(f"\nRunning test classification for {test_image} to stage {test_stage} (model={test_model})")
            from app import run_classification_on_uploaded
            try:
                rows, put_res = run_classification_on_uploaded(test_image, test_stage, model_name=test_model)
                print('Classification rows:', rows)
                print('PUT result:', put_res)
            except Exception as e:
                print('Classification attempt failed:', e)
                traceback.print_exc()
        else:
            print('\nSet TEST_IMAGE and IMAGE_STAGE env vars to attempt a live classification test.')

    finally:
        try:
            csf.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
