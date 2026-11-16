"""ChargeAssert web interface. Live requests stay on the Databricks server."""
from __future__ import annotations

import argparse
import importlib
import logging
import mimetypes
import os
import secrets
import sys
import threading

from flask import Flask, jsonify, render_template, request, session
from werkzeug.exceptions import HTTPException

# Windows may register JavaScript as text/plain; nosniff correctly rejects it.
mimetypes.add_type("text/javascript", ".js")


def create_app(service=None, *, preview=False):
    web = Flask(__name__)
    web.config.update(
        SECRET_KEY=os.environ.get("FLASK_SECRET_KEY") or secrets.token_hex(32),
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=not preview,
        MAX_CONTENT_LENGTH=16 * 1024,
    )
    service_lock = threading.Lock()
    web.extensions["simulation_service"] = service

    def get_service():
        with service_lock:
            if web.extensions["simulation_service"] is None:
                name = "preview" if preview else "service"
                module = importlib.import_module(f"{__package__}.{name}" if __package__ else name)
                if hasattr(module, "AppError"):
                    web.extensions["simulation_error"] = module.AppError
                web.extensions["simulation_service"] = module.SimulationService()
            value = web.extensions["simulation_service"]
            module = sys.modules.get(type(value).__module__)
            if module is not None and hasattr(module, "AppError"):
                web.extensions["simulation_error"] = module.AppError
            return value

    @web.after_request
    def security_headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; font-src 'self'; "
            "object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'self'"
        )
        return response

    @web.errorhandler(Exception)
    def handle_error(error):
        if isinstance(error, HTTPException):
            return jsonify(error=error.description), error.code
        expected = web.extensions.get("simulation_error")
        if expected is not None and isinstance(error, expected):
            return jsonify(error=str(error)), error.status
        web.logger.exception("Simulation request failed")
        return jsonify(error="The request could not be completed. Try again, or check the app logs."), 500

    @web.get("/")
    def index():
        return render_template("index.html")

    @web.get("/api/config")
    def config():
        if "csrf_token" not in session:
            session["csrf_token"] = secrets.token_urlsafe(32)
        result = dict(get_service().get_config())
        result["csrf_token"] = session["csrf_token"]
        return jsonify(result)

    @web.get("/api/runs")
    def list_runs():
        return jsonify(runs=get_service().list_runs())

    @web.post("/api/runs")
    def start_run():
        expected = session.get("csrf_token", "")
        supplied = request.headers.get("X-CSRF-Token", "")
        if not expected or not secrets.compare_digest(expected, supplied):
            return jsonify(error="Your session needs to be refreshed. Reload this page before starting a run."), 403
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error="Send the simulation settings as a JSON object."), 400
        return jsonify(get_service().start_run(payload)), 202

    @web.get("/api/runs/<int:run_id>")
    def get_run(run_id):
        if run_id < 1:
            return jsonify(error="Choose a valid run."), 400
        return jsonify(get_service().get_run(run_id))

    @web.get("/api/runs/<int:run_id>/results")
    def get_results(run_id):
        if run_id < 1:
            return jsonify(error="Choose a valid run."), 400
        return jsonify(get_service().get_results(run_id))

    return web


app = create_app()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ChargeAssert app")
    parser.add_argument("--preview", action="store_true", help="Use clearly labelled sample data, without Databricks")
    parser.add_argument("--port", type=int, default=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    create_app(preview=args.preview).run(host="127.0.0.1" if args.preview else "0.0.0.0", port=args.port, debug=False)
