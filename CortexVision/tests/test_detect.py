import os
import hashlib
import tempfile
import shutil
import pytest

from app import run_classification_on_uploaded
from snowflake_conn import CustomSnowflake


def _have_snowflake_creds():
    return bool(os.environ.get('SNOWFLAKE_ACCOUNT') and os.environ.get('SNOWFLAKE_USER') and os.environ.get('SNOWFLAKE_PASSWORD'))


@pytest.mark.skipif(not _have_snowflake_creds(), reason="Snowflake credentials not configured")
def test_detect_sample_images():
    """Take a few images from the local `images/` folder, upload them and assert
    the detection helper returns a matching caption from IMAGE_METADATA.

    This test will skip images that have no metadata entry.
    """
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
    except Exception as e:
        pytest.skip(f"Unable to connect to Snowflake: {e}")

    # Find a small sample of images (one per class folder) under images/
    repo_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    images_root = os.path.join(repo_root, 'images')
    if not os.path.isdir(images_root):
        pytest.skip("No images/ directory present in repository")

    samples = []
    for root, dirs, files in os.walk(images_root):
        for f in files:
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                samples.append(os.path.join(root, f))
                break
        if len(samples) >= 5:
            break

    assert samples, "No sample images found under images/"

    stage_name = os.environ.get('IMAGE_STAGE', '@VISIONDB.HACKATHON_SCHEMA.IMAGE_STAGE')

    tested = 0
    for local_path in samples:
        basename = os.path.basename(local_path)
        # Attempt to find expected caption via IMAGE_METADATA (basename match)
        try:
            rows, _ = csf.run_command(
                "SELECT CAPTION FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE FILE_PATH LIKE %s LIMIT 1",
                params=(f"%/{basename}",), fetch=True,
            )
        except Exception as e:
            pytest.skip(f"DB query failed when looking up metadata for {basename}: {e}")

        if not rows:
            # No metadata for this file — skip rather than failing the entire test
            continue

        expected_caption = rows[0][0]

        # Run classification helper which will PUT the file and try embedding/fallbacks
        try:
            result_rows, put_res = run_classification_on_uploaded(local_path, stage_name, model_name=None)
        except Exception as e:
            pytest.fail(f"run_classification_on_uploaded failed for {local_path}: {e}")

        assert result_rows, f"No classification rows returned for {local_path}"
        top_cats = [r[0] for r in result_rows]
        assert expected_caption in top_cats, f"Expected caption '{expected_caption}' not in top results {top_cats} for {basename}"
        tested += 1

    assert tested > 0, "No images with metadata were available to test"


@pytest.mark.skipif(not _have_snowflake_creds(), reason="Snowflake credentials not configured")
def test_detect_camera_like_hash_match():
    """Simulate a camera upload (no original filename). Save a sample image to a temp
    file with a random name and confirm the content-hash fallback finds the trained caption.
    The test will skip if IMAGE_METADATA does not contain a FILE_HASH column.
    """
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
    except Exception as e:
        pytest.skip(f"Unable to connect to Snowflake: {e}")

    # Check if FILE_HASH column exists
    try:
        rows, _ = csf.run_command(
            "SELECT COUNT(*) FROM VISIONDB.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='HACKATHON_SCHEMA' AND TABLE_NAME='IMAGE_METADATA' AND COLUMN_NAME='FILE_HASH'",
            fetch=True,
        )
        has_file_hash = bool(rows and rows[0][0] > 0)
    except Exception:
        has_file_hash = False

    if not has_file_hash:
        pytest.skip("IMAGE_METADATA.FILE_HASH column not present; skipping hash-match test")

    # pick one sample image that has metadata
    repo_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    images_root = os.path.join(repo_root, 'images')
    sample = None
    for root, dirs, files in os.walk(images_root):
        for f in files:
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                sample = os.path.join(root, f)
                break
        if sample:
            break

    if not sample:
        pytest.skip("No images available to test hash-match")

    # compute hash and ensure there's a metadata row for it
    with open(sample, 'rb') as fh:
        file_hash = hashlib.sha256(fh.read()).hexdigest()

    try:
        rows, _ = csf.run_command(
            "SELECT CAPTION FROM VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA WHERE FILE_HASH = %s LIMIT 1",
            params=(file_hash,), fetch=True,
        )
    except Exception as e:
        pytest.skip(f"DB query for FILE_HASH failed: {e}")

    if not rows:
        pytest.skip("No metadata row with matching FILE_HASH found; ensure metadata was ingested with hashes")

    expected_caption = rows[0][0]

    # save to temp file with random name (no original basename)
    tmpd = tempfile.mkdtemp()
    try:
        tmpf = os.path.join(tmpd, 'random_upload.jpg')
        shutil.copyfile(sample, tmpf)

        stage_name = os.environ.get('IMAGE_STAGE', '@VISIONDB.HACKATHON_SCHEMA.IMAGE_STAGE')
        try:
            result_rows, put_res = run_classification_on_uploaded(tmpf, stage_name, model_name=None)
        except Exception as e:
            pytest.fail(f"run_classification_on_uploaded failed for temp file: {e}")

        assert result_rows, "No classification rows returned for temp camera-like upload"
        top_cats = [r[0] for r in result_rows]
        assert expected_caption in top_cats, f"Expected caption '{expected_caption}' not in top results {top_cats} for camera-like upload"
    finally:
        try:
            shutil.rmtree(tmpd)
        except Exception:
            pass
