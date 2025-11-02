from app import app
from flask import url_for

with app.test_request_context():
    print('diagnostics ->', url_for('admin.admin_diagnostics'))
    print('panel ->', url_for('admin.admin_panel'))
