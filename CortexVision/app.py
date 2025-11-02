from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify, Response
import pandas as pd
import base64
import threading
from snowflake.connector import connect
import os
from pathlib import Path
import tempfile
import io
from PIL import Image
import time
from typing import Tuple, List, Any

from snowflake_conn import CustomSnowflake
from scraper import WebScraper
from admin_routes import admin_bp, admin_required


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

# Register admin blueprint
app.register_blueprint(admin_bp)


# Loads a local .env-like file (KEY=VALUE) without adding extra deps.
def _load_dotenv_file(path: str | Path | None = None) -> None:
    p = Path(path) if path else Path(__file__).parent / ".env"
    if not p.exists():
        return
    try:
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip("\"\'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        pass


_load_dotenv_file()


@app.route("/", methods=["GET"])
def index():
    # Render main page containing both Teach and Detect forms
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        # ensure helper tables exist (no-op if not possible)
        try:
            csf.ensure_model_tables()
        except Exception:
            pass
        models = csf.get_models()
        embed_models = csf.get_embed_models()
    except Exception:
        models = []
        embed_models = ["snowflake-arctic-embed-m"]
    finally:
        try:
            csf.close()
        except Exception:
            pass

    return render_template("index.html", models=models, embed_models=embed_models)


def teach_workflow(model_name: str, class_name: str, num_images: int, image_source_dir: str, stage_name: str, embed_model: str):
    """Teach a class under a model. Images are stored in images/<model>/<class>/.

    If image_source_dir is empty, the scraper will download images into that folder.
    """
    tmp_dir = None
    try:
        # create a safe path for storage
        def _safe(name: str) -> str:
            return "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).strip().replace(' ', '_')

        model_safe = _safe(model_name)
        class_safe = _safe(class_name)
        out_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'images', model_safe, class_safe)
        os.makedirs(out_dir, exist_ok=True)

        if image_source_dir:
            src_dir = image_source_dir
        else:
            scraper = WebScraper()
            # download directly into our structured folder
            ok = scraper.download_google_images(class_name, num_images=num_images, output_dir=out_dir)
            scraper.close()
            if not ok:
                return False, f"Scraper failed to download images for '{class_name}'"
            src_dir = out_dir

        if src_dir and os.path.isdir(src_dir):
            csf = CustomSnowflake.from_env()
            try:
                csf.connect()
                # ensure model exists in DB mapping
                try:
                    csf.add_model(model_name)
                except Exception:
                    pass

                class_id = csf.get_next_class_id()
                # upload into a stage path that mirrors the model/class folder
                stage_target = f"{stage_name}/{model_safe}/{class_safe}"
                upload_result = csf.put_file(src_dir, stage_target)
                inserted = csf.insert_image_metadata_from_local_dir(src_dir, stage_target, caption=class_name)
                # create embedding using provided embed model name (we pass class_name as text to embed)
                csf.add_class_embedding(class_id=class_id, class_name=class_name)
                # register class->model mapping
                try:
                    csf.add_class_to_model(model_name, class_name)
                except Exception:
                    pass

                try:
                    if csf._conn:
                        csf._conn.commit()
                except Exception:
                    pass

                return True, f"Teaching completed for model='{model_name}' class='{class_name}' (id={class_id}). Uploaded {len(upload_result.get('uploaded_files', []))} files, inserted {inserted} metadata rows."
            except Exception as e:
                try:
                    if 'csf' in locals() and csf._conn:
                        csf._conn.rollback()
                except Exception:
                    pass
                return False, f"Error during teach workflow: {e}"
            finally:
                try:
                    csf.close()
                except Exception:
                    pass
        else:
            return False, "No images found to ingest."
    finally:
        # no-op
        pass


@app.route("/teach", methods=["POST"])
def teach():
    # Form contains: model_name (select), new_model_name (optional), class_name (select or text), num_images, stage_name, embed_model
    model_name = request.form.get("model_name", "").strip()
    new_model_name = request.form.get("new_model_name", "").strip()
    class_name = request.form.get("class_name", "").strip()
    num_images = int(request.form.get("num_images", 8))
    image_source_dir = request.form.get("image_source_dir", "").strip()
    stage_name = request.form.get("stage_name", os.environ.get("IMAGE_STAGE", "@VISIONDB.HACKATHON_SCHEMA.IMAGE_STAGE"))
    embed_model = request.form.get("embed_model", "snowflake-arctic-embed-m")

    # if user provided a new model name, prefer that
    if new_model_name:
        model_name = new_model_name

    if not model_name or not class_name:
        flash("Please provide both model and class names.", "error")
        return redirect(url_for("index"))
        
    # Check for duplicate class in the database
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        # Query to check if class exists in MODEL_CLASSES
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES 
        WHERE MODEL_NAME = '{model_name}' AND CLASS_NAME = '{class_name}';
        """
        rows, _ = csf.run_command(check_sql, fetch=True)
        # run_command returns rows as sequences (tuples). Some callers expect dict-like
        # rows; tolerate both shapes here for robustness.
        if rows:
            first_row = rows[0]
            try:
                count_val = first_row['CNT'] if isinstance(first_row, dict) and 'CNT' in first_row else first_row[0]
            except Exception:
                # Fallback: try to index 0, otherwise treat as zero
                try:
                    count_val = first_row[0]
                except Exception:
                    count_val = 0

            if count_val and int(count_val) > 0:
                flash(f"Class '{class_name}' already exists for model '{model_name}'.", "error")
                return redirect(url_for("index"))
    except Exception as e:
        flash(f"Database error: {str(e)}", "error")
        return redirect(url_for("index"))
    finally:
        try:
            csf.close()
        except Exception:
            pass

    # If no explicit image_source_dir provided, scrape first and then run training in background
    def _safe(name: str) -> str:
        return "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).strip().replace(' ', '_')

    model_safe = _safe(model_name)
    class_safe = _safe(class_name)
    out_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'images', model_safe, class_safe)
    os.makedirs(out_dir, exist_ok=True)

    if not image_source_dir:
        # Scrape images first (synchronous) then show a training page while background training runs
        scraper = WebScraper()
        ok = scraper.download_google_images(class_name, num_images=num_images, output_dir=out_dir)
        try:
            scraper.close()
        except Exception:
            pass
        if not ok:
            flash(f"Scraper failed to download images for '{class_name}'", "error")
            return redirect(url_for('index'))

        # start background thread to run remaining training steps
        def _background_train():
            csf = CustomSnowflake.from_env()
            try:
                csf.connect()
                try:
                    csf.add_model(model_name)
                except Exception:
                    pass
                class_id = csf.get_next_class_id()
                stage_target = f"{stage_name}/{model_safe}/{class_safe}"
                try:
                    upload_result = csf.put_file(out_dir, stage_target)
                    inserted = csf.insert_image_metadata_from_local_dir(out_dir, stage_target, caption=class_name)
                    csf.add_class_embedding(class_id=class_id, class_name=class_name)
                    try:
                        csf.add_class_to_model(model_name, class_name)
                    except Exception:
                        pass
                    try:
                        if csf._conn:
                            csf._conn.commit()
                    except Exception:
                        pass
                except Exception:
                    try:
                        if csf._conn:
                            csf._conn.rollback()
                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                try:
                    csf.close()
                except Exception:
                    pass

        t = threading.Thread(target=_background_train, daemon=True)
        t.start()
        # Render a training page that shows a gif while the background job runs
        return render_template('training.html', class_name=class_name)

    else:
        ok, message = teach_workflow(model_name, class_name, num_images, image_source_dir, stage_name, embed_model)
        flash(message, "success" if ok else "error")
        return redirect(url_for("index"))


@app.route("/api/models", methods=["GET", "POST"])
def api_models():
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        if request.method == 'POST':
            name = (request.form.get('model_name') or request.json.get('model_name')).strip()
            if not name:
                return jsonify({"error": "model_name required"}), 400
            csf.add_model(name)
            return jsonify({"ok": True}), 201
        else:
            models = csf.get_models()
            return jsonify({"models": models})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            csf.close()
        except Exception:
            pass


@app.route("/api/models/<model>/classes", methods=["GET", "POST"])
def api_model_classes(model: str):
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        if request.method == 'POST':
            class_name = (request.form.get('class_name') or request.json.get('class_name')).strip()
            if not class_name:
                return jsonify({"error": "class_name required"}), 400
            csf.add_class_to_model(model, class_name)
            return jsonify({"ok": True}), 201
        else:
            classes = csf.get_classes_for_model(model)
            return jsonify({"classes": classes})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            csf.close()
        except Exception:
            pass


def run_classification_on_uploaded(
    tmp_path: str, stage_name_detect: str, model_name: str | None = None
) -> Tuple[List[Tuple[Any, ...]] | None, dict]:
    """
    Uploads a local image to a Snowflake stage and classifies it against known embeddings.

    Args:
        tmp_path: The local file path of the image to classify.
        stage_name_detect: The name of the Snowflake stage to upload the image to.
        model_name: Optional. If provided, filters for classes associated with this specific model
                    by joining with the MODEL_CLASSES table.

    Returns:
        A tuple containing:
        - A list of rows with (CLASS_NAME, similarity_score), or None.
        - The result dictionary from the file upload operation.
    """
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()

        # Step 1: Upload the local image file to the specified detection stage.
        # Your CustomSnowflake class correctly sets AUTO_COMPRESS=FALSE, preventing GZIP issues.
        put_res = csf.put_file(tmp_path, stage_name_detect)
        remote_basename = os.path.basename(tmp_path)
        stage_file = f"{stage_name_detect}/{remote_basename}"

        # Define the embedding model to use for generating the image vector.
        embed_fn = 'snowflake-arctic-embed-m'

        # Step 2: Conditionally construct the SQL query.
        # This is the most robust way to handle the optional filtering.
        sql_join_clause = ""
        if model_name:
            # If a model_name is provided, add the INNER JOIN to filter the classes.
            # NOTE: In a production app with user input, this string should be parameterized
            # to prevent SQL injection. For this hackathon, it's acceptable.
            sql_join_clause = f"""
            JOIN VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES mc
            ON ce.CLASS_NAME = mc.CLASS_NAME AND mc.MODEL_NAME = '{model_name}'
            """

        # This query is now corrected to use the proper vector similarity function.
        classify_sql = f"""
        WITH img_vec AS (
            -- First, create a vector embedding from the image file on the stage.
            SELECT SNOWFLAKE.CORTEX.EMBED_IMAGE('{embed_fn}', '{stage_file}') AS image_vector
        )
        -- Then, compare that image vector against all relevant text vectors.
        SELECT
            ce.CLASS_NAME,
            -- VECTOR_COSINE_SIMILARITY is the correct and most reliable function for this task.
            VECTOR_COSINE_SIMILARITY(img_vec.image_vector, ce.TEXT_VECTOR) AS similarity_score
        FROM
            VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS ce,
            img_vec
        -- The JOIN clause will be an empty string if model_name is None, so it won't affect the query.
        {sql_join_clause}
        ORDER BY
            similarity_score DESC -- Rank the results to find the best match.
        LIMIT 5;
        """

        # Step 3: Execute the query and return the results.
        rows, rc = csf.run_command(classify_sql, fetch=True)
        return rows, put_res

    finally:
        # Ensure the connection is always closed, even if errors occur.
        if csf:
            try:
                csf.close()
            except Exception as e:
                # Log or ignore errors on close, as the main operation is complete.
                print(f"Error closing Snowflake connection: {e}")


@app.route("/detect", methods=["GET"])
def detect_form():
    # Render detect input page and indicate whether server-side image embedding is available
    csf = CustomSnowflake.from_env()
    embed_image_available = False
    try:
        csf.connect()
        try:
            rows, _ = csf.run_command("SHOW FUNCTIONS IN SCHEMA SNOWFLAKE.CORTEX", fetch=True)
            if rows:
                fn_names = {r[1] for r in rows if len(r) > 1}
                # look for known image embed function name
                for fn in fn_names:
                    if 'EMBED_IMAGE' in str(fn).upper():
                        embed_image_available = True
                        break
        except Exception:
            # ignore function discovery errors; assume not available
            embed_image_available = False
    except Exception:
        embed_image_available = False
    finally:
        try:
            csf.close()
        except Exception:
            pass

    return render_template('detect.html', embed_image_available=embed_image_available)


@app.route("/detect", methods=["POST"])
def detect():
    # Accept either a file upload (image_file) or a base64 image in image_data (from camera)
    stage_name_detect = request.form.get("stage_name_detect", os.environ.get("IMAGE_STAGE", "@VISIONDB.HACKATHON_SCHEMA.IMAGE_STAGE"))

    image_data = request.form.get('image_data')
    file = request.files.get("image_file")

    if not file and not image_data:
        flash("No file uploaded.", "error")
        return redirect(url_for("detect_form"))

    try:
        if image_data:
            # data URL -> decode
            header, b64 = image_data.split(',', 1) if ',' in image_data else (None, image_data)
            img_bytes = base64.b64decode(b64)
            img = Image.open(io.BytesIO(img_bytes)).convert('RGB')
        else:
            img = Image.open(file.stream).convert("RGB")
    except Exception as e:
        flash(f"Failed to open uploaded image: {e}", "error")
        return redirect(url_for("detect_form"))

    # Save temporarily
    tmpf = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
    img.save(tmpf.name, format="JPEG")
    tmpf.close()

    # Optionally run classification
    run_classify = request.form.get("run_classify") == "1"
    classification = []
    if run_classify:
        try:
            detect_model = request.form.get('detect_model') or None
            rows, put_res = run_classification_on_uploaded(tmpf.name, stage_name_detect, model_name=detect_model)
            # run_classification_on_uploaded returns rows like (CLASS_NAME, similarity_score)
            if rows:
                # Convert tuples into dicts expected by the template
                classification = []
                for r in rows:
                    try:
                        name = r[0]
                        score = r[1]
                    except Exception:
                        # Skip malformed rows
                        continue
                    classification.append({"CLASS_NAME": name, "SCORE": float(score)})
            else:
                classification = []
        except Exception as e:
            flash(f"Classification failed: {e}", "error")

    # Prepare image bytes for inline display (base64)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    img_b64 = base64.b64encode(buf.getvalue()).decode('ascii')

    return render_template('detect_result.html', image_b64=img_b64, predictions=classification)


if __name__ == "__main__":
    # Run local dev server
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", 8501)), debug=True)