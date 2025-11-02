"""Reset the Vision dataset: wipe AI tables and delete local images.

This script is destructive. It will only run if the environment variable
CONFIRM_RESET is set to '1'. It performs the following actions:
 - DELETE FROM CLASS_EMBEDDINGS, IMAGE_METADATA, MODEL_CLASSES, AI_MODELS
 - Remove all files and subdirectories under the local `images/` folder

Run only if you are sure. The script logs actions and errors.
"""
import os
import shutil
import traceback
from snowflake_conn import CustomSnowflake


def ensure_confirm():
    return os.environ.get('CONFIRM_RESET') == '1'


def wipe_db(csf):
    tables = [
        'VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS',
        'VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA',
        'VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES',
        'VISIONDB.HACKATHON_SCHEMA.AI_MODELS',
    ]
    for t in tables:
        sql = f"DELETE FROM {t}"
        try:
            print(f"Running: {sql}")
            csf.run_command(sql, fetch=False)
            print(f"Deleted rows from {t}")
        except Exception as e:
            print(f"Failed to delete from {t}: {e}")
            traceback.print_exc()


def wipe_local_images(root_path):
    images_dir = os.path.join(root_path, 'images')
    if not os.path.exists(images_dir):
        print(f"Local images dir not found: {images_dir}")
        return
    # Remove everything under images/ but keep the images directory itself
    for name in os.listdir(images_dir):
        path = os.path.join(images_dir, name)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.remove(path)
            else:
                shutil.rmtree(path)
            print(f"Removed {path}")
        except Exception as e:
            print(f"Failed to remove {path}: {e}")
            traceback.print_exc()


def main():
    if not ensure_confirm():
        print("CONFIRM_RESET is not set to '1'. Exiting without changes.")
        return

    csf = CustomSnowflake.from_env()
    try:
        print("Connecting to Snowflake...")
        csf.connect()
        wipe_db(csf)
    except Exception as e:
        print(f"DB reset failed: {e}")
        traceback.print_exc()
    finally:
        try:
            csf.close()
        except Exception:
            pass

    # Wipe local images
    root = os.path.abspath(os.path.dirname(__file__))
    print(f"Wiping local images under {os.path.join(root,'images')}")
    wipe_local_images(root)

    print("Reset complete. Tables cleared and local images removed.")


if __name__ == '__main__':
    main()
