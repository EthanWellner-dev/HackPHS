from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify
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
import random
from flask_sock import Sock
import json

from snowflake_conn import CustomSnowflake
from scraper import WebScraper

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")
sock = Sock(app)

# Store training progress for each session
training_sessions = {}

# Class tracking to prevent duplicates
trained_classes = set()

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

def get_sample_training_images(class_name, num_images=20):
    """Get a random sample of training images for animation"""
    img_dir = Path(__file__).parent / 'images' / 'Mountain_Detector' / class_name
    if not img_dir.exists():
        return []
    
    image_files = list(img_dir.glob('*.jpg')) + list(img_dir.glob('*.png'))
    return random.sample(image_files, min(num_images, len(image_files)))

def process_training_images(class_name, ws):
    """Process training images and send progress updates via WebSocket"""
    session_id = id(threading.current_thread())
    training_sessions[session_id] = {
        'processed': 0,
        'total': 0,
        'stage': 'initializing'
    }
    
    try:
        # Check if class already exists
        if class_name in trained_classes:
            ws.send(json.dumps({
                'type': 'error',
                'message': f'Class {class_name} has already been trained'
            }))
            return
        
        # Get sample images for animation
        sample_images = get_sample_training_images(class_name)
        if sample_images:
            image_urls = [f'/training_image/{class_name}/{img.name}' for img in sample_images]
            ws.send(json.dumps({
                'type': 'images',
                'images': image_urls
            }))
        
        # Simulate training progress (replace with actual training logic)
        scraper = WebScraper()
        images = scraper.get_images(class_name, limit=50)
        total_images = len(images)
        training_sessions[session_id]['total'] = total_images
        
        # Process images
        for i, img_data in enumerate(images, 1):
            training_sessions[session_id].update({
                'processed': i,
                'stage': 'Processing images'
            })
            ws.send(json.dumps({
                'type': 'progress',
                'processed': i,
                'total': total_images,
                'stage': 'Processing images'
            }))
            time.sleep(0.1)  # Simulate processing time
        
        # Add to trained classes set
        trained_classes.add(class_name)
        
        # Send completion message
        ws.send(json.dumps({
            'type': 'complete'
        }))
        
    except Exception as e:
        ws.send(json.dumps({
            'type': 'error',
            'message': str(e)
        }))
    finally:
        if session_id in training_sessions:
            del training_sessions[session_id]

@sock.route('/ws/training')
def training_socket(ws):
    """WebSocket endpoint for training progress updates"""
    while True:
        try:
            message = ws.receive()
            data = json.loads(message)
            if data['action'] == 'start_training':
                thread = threading.Thread(
                    target=process_training_images,
                    args=(data['class_name'], ws)
                )
                thread.start()
        except Exception as e:
            print(f"WebSocket error: {e}")
            break

@app.route('/training_image/<class_name>/<image_name>')
def get_training_image(class_name, image_name):
    """Serve training images for animation"""
    img_path = Path(__file__).parent / 'images' / 'Mountain_Detector' / class_name / image_name
    if img_path.exists():
        return send_file(img_path)
    return '', 404