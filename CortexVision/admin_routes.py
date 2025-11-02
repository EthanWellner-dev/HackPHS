
import functools
import os
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response, current_app
from snowflake_conn import CustomSnowflake

# Create Blueprint
admin_bp = Blueprint('admin', __name__)

# Add after the app initialization
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")  # Change this in production!

def admin_required(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_admin_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated_function

def check_admin_auth(username, password):
    return username == "admin" and password == ADMIN_PASSWORD

def authenticate():
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )

def get_storage_stats():
    """Calculate storage usage for images"""
    total_size = 0
    images_path = os.path.join(current_app.root_path, 'images')
    for root, dirs, files in os.walk(images_path):
        for f in files:
            fp = os.path.join(root, f)
            total_size += os.path.getsize(fp)
    return format_size(total_size)

def format_size(size):
    """Format size in bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"

@admin_bp.route("/admin")
@admin_required
def admin_panel():
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()

        # Get models and their class counts
        models_query = """
        SELECT m.MODEL_NAME as name,
               COUNT(DISTINCT mc.CLASS_NAME) as class_count
        FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS m
        LEFT JOIN VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES mc
            ON m.MODEL_NAME = mc.MODEL_NAME
        GROUP BY m.MODEL_NAME;
        """
        models_rows, _ = csf.run_command(models_query, fetch=True)
        # Convert SQL result tuples to dicts expected by the template
        models = []
        if models_rows:
            for r in models_rows:
                # r -> (MODEL_NAME, CLASS_COUNT)
                models.append({
                    'name': r[0],
                    'class_count': int(r[1]) if r[1] is not None else 0,
                })

        # Get classes with their stats
        # Query classes and include image counts and earliest added timestamp (if available)
        # IMAGE_METADATA in some deployments may not have CREATED_AT; only request image counts here
        classes_query = """
        SELECT
            mc.MODEL_NAME,
            mc.CLASS_NAME as name,
            COUNT(im.FILE_PATH) as image_count
        FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES mc
        LEFT JOIN VISIONDB.HACKATHON_SCHEMA.IMAGE_METADATA im
            ON im.CAPTION = mc.CLASS_NAME
        GROUP BY mc.MODEL_NAME, mc.CLASS_NAME
        ORDER BY mc.MODEL_NAME, mc.CLASS_NAME;
        """
        classes_rows, _ = csf.run_command(classes_query, fetch=True)
        classes = []
        if classes_rows:
            for r in classes_rows:
                # r -> (MODEL_NAME, CLASS_NAME, IMAGE_COUNT, CREATED_AT)
                classes.append({
                    'model_name': r[0],
                    'name': r[1],
                    'image_count': int(r[2]) if r[2] is not None else 0,
                })

        # System stats
        system_stats = {
            'storage_used': get_storage_stats(),
            'total_models': len(models),
            'total_classes': len(classes)
        }

        return render_template('admin.html',
                             models=models,
                             classes=classes,
                             system_stats=system_stats)
    finally:
        try:
            csf.close()
        except Exception:
            pass


@admin_bp.route('/admin/diagnostics')
@admin_required
def admin_diagnostics():
    """Run quick diagnostics: table counts and presence of Snowflake functions used by the app.

    This helps operators see whether UDFs like EMBED_IMAGE are present.
    """
    csf = CustomSnowflake.from_env()
    diagnostics = {
        'ok': True,
        'errors': [],
        'counts': {},
        'functions': [],
    }
    try:
        csf.connect()
        # Table counts
        for tbl in ['CLASS_EMBEDDINGS', 'IMAGE_METADATA', 'MODEL_CLASSES', 'AI_MODELS']:
            try:
                rows, _ = csf.run_command(f"SELECT COUNT(*) FROM VISIONDB.HACKATHON_SCHEMA.{tbl}", fetch=True)
                diagnostics['counts'][tbl] = rows[0][0] if rows else None
            except Exception as e:
                diagnostics['counts'][tbl] = None
                diagnostics['errors'].append(f"Count error for {tbl}: {e}")

        # Check for Cortex functions via SHOW FUNCTIONS IN SCHEMA; fall back gracefully if not allowed
        try:
            rows, _ = csf.run_command("SHOW FUNCTIONS IN SCHEMA SNOWFLAKE.CORTEX", fetch=True)
            if rows:
                # rows contain function metadata; find names
                fn_names = {r[1] for r in rows if len(r) > 1}
                diagnostics['functions'] = sorted(list(fn_names))
            else:
                diagnostics['functions'] = []
        except Exception as e:
            diagnostics['functions'] = []
            diagnostics['errors'].append(f"Function discovery failed: {e}")

    finally:
        try:
            csf.close()
        except Exception:
            pass

    return render_template('admin_diagnostics.html', diagnostics=diagnostics)

@admin_bp.route("/admin/delete_model", methods=["POST"])
@admin_required
def admin_delete_model():
    model_name = request.form.get("model_name")
    if not model_name:
        flash("Model name is required", "error")
        return redirect(url_for("admin.admin_panel"))

    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        
        # Delete from model_classes first (foreign key constraint)
        csf.run_command(f"""
        DELETE FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES 
        WHERE MODEL_NAME = '{model_name}';
        """)
            
        # Delete from AI_MODELS
        csf.run_command(f"""
        DELETE FROM VISIONDB.HACKATHON_SCHEMA.AI_MODELS 
        WHERE MODEL_NAME = '{model_name}';
        """)

        # Delete associated files
        model_dir = os.path.join(current_app.root_path, 'images', model_name)
        if os.path.exists(model_dir):
            for root, dirs, files in os.walk(model_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(model_dir)

        flash(f"Model {model_name} deleted successfully", "success")
    except Exception as e:
        flash(f"Error deleting model: {str(e)}", "error")
    finally:
        try:
            csf.close()
        except Exception:
            pass
    return redirect(url_for("admin.admin_panel"))

@admin_bp.route("/admin/delete_class", methods=["POST"])
@admin_required
def admin_delete_class():
    class_name = request.form.get("class_name")
    model_name = request.form.get("model_name")
    
    if not class_name or not model_name:
        flash("Class name and model name are required", "error")
        return redirect(url_for("admin.admin_panel"))

    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        
        # Delete from class_embeddings
        csf.run_command(f"""
        DELETE FROM VISIONDB.HACKATHON_SCHEMA.CLASS_EMBEDDINGS 
        WHERE CLASS_NAME = '{class_name}';
        """)
        
        # Delete from model_classes
        csf.run_command(f"""
        DELETE FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES 
        WHERE MODEL_NAME = '{model_name}' AND CLASS_NAME = '{class_name}';
        """)

        # Delete associated files
        class_dir = os.path.join(current_app.root_path, 'images', model_name, class_name)
        if os.path.exists(class_dir):
            for f in os.listdir(class_dir):
                os.remove(os.path.join(class_dir, f))
            os.rmdir(class_dir)

        flash(f"Class {class_name} deleted successfully", "success")
    except Exception as e:
        flash(f"Error deleting class: {str(e)}", "error")
    finally:
        try:
            csf.close()
        except Exception:
            pass
    return redirect(url_for("admin.admin_panel"))

@admin_bp.route("/admin/cleanup_images", methods=["POST"])
@admin_required
def admin_cleanup_images():
    """Remove image files that don't have corresponding database entries"""
    csf = CustomSnowflake.from_env()
    try:
        csf.connect()
        
        # Get all valid class names from database
        classes_query = "SELECT DISTINCT CLASS_NAME FROM VISIONDB.HACKATHON_SCHEMA.MODEL_CLASSES;"
        valid_classes, _ = csf.run_command(classes_query, fetch=True)
        # run_command returns tuples; CLASS_NAME is first column
        valid_class_names = {row[0] for row in valid_classes} if valid_classes else set()

        # Walk through image directory and remove invalid files/directories
        images_dir = os.path.join(current_app.root_path, 'images')
        files_removed = 0
        dirs_removed = 0

        for root, dirs, files in os.walk(images_dir, topdown=False):
            # Get class name from path
            class_name = os.path.basename(root)
            
            # If this directory represents a class and it's not in our valid list
            if root != images_dir and class_name not in valid_class_names:
                # Remove all files
                for f in files:
                    os.remove(os.path.join(root, f))
                    files_removed += 1
                # Remove the directory
                os.rmdir(root)
                dirs_removed += 1

        flash(f"Cleanup complete: removed {files_removed} files and {dirs_removed} directories", "success")
    except Exception as e:
        flash(f"Error during cleanup: {str(e)}", "error")
    finally:
        try:
            csf.close()
        except Exception:
            pass
    return redirect(url_for("admin.admin_panel"))