"""Login / register against public.User using Supabase service role; returns backend-signed JWT."""

import traceback

from flask import jsonify, request

from . import auth_bp
from ..config import Config
from ..services.user_auth_db import email_exists, get_user_by_email, insert_user
from ..utils.logger import get_logger
from ..utils.mirofish_jwt import issue_session_token

logger = get_logger("mirofish.api.auth")


def _auth_unavailable():
    if not Config.SUPABASE_URL or not Config.SUPABASE_SERVICE_ROLE_KEY:
        return jsonify(
            {
                "success": False,
                "error": "Server is missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY for database auth.",
            }
        ), 503
    return None


@auth_bp.route("/login", methods=["POST"])
def auth_login():
    try:
        err = _auth_unavailable()
        if err:
            return err
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip()
        password = data.get("password") or ""
        if not email or not password:
            return jsonify({"success": False, "error": "email and password are required"}), 400

        user = get_user_by_email(email)
        if not user or user.get("Password") != password:
            return jsonify({"success": False, "error": "Invalid email or password"}), 401

        token = issue_session_token(str(user["id"]))
        safe_user = {"id": user["id"], "Username": user.get("Username"), "Email": user.get("Email")}
        return jsonify({"success": True, "data": {"access_token": token, "user": safe_user}})
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@auth_bp.route("/register", methods=["POST"])
def auth_register():
    try:
        err = _auth_unavailable()
        if err:
            return err
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or data.get("Username") or "").strip()
        email = (data.get("email") or data.get("Email") or "").strip()
        password = data.get("password") or data.get("Password") or ""
        if not username or not email or not password:
            return jsonify({"success": False, "error": "username, email, and password are required"}), 400
        if len(password) < 6:
            return jsonify({"success": False, "error": "password must be at least 6 characters"}), 400

        if email_exists(email):
            return jsonify({"success": False, "error": "An account with this email already exists"}), 409

        row = insert_user(username=username, email=email, password=password)
        token = issue_session_token(str(row["id"]))
        safe_user = {"id": row["id"], "Username": row.get("Username"), "Email": row.get("Email")}
        return jsonify({"success": True, "data": {"access_token": token, "user": safe_user}})
    except Exception as e:
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500
