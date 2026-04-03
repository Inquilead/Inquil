"""
WSGI entry for production (e.g. Gunicorn on Render).
"""

from app import create_app

app = create_app()
