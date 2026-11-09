"""HTTP boundaries for the app, independent of a Databricks workspace."""
import importlib.util
import unittest
from unittest.mock import Mock
from uuid import uuid4

HAS_FLASK = importlib.util.find_spec("flask") is not None
if HAS_FLASK:
    from app.app import create_app
    from app.preview import SimulationService


@unittest.skipUnless(HAS_FLASK, "Install app/requirements.txt to run the Flask route tests")
class SimulationWebTests(unittest.TestCase):
    def setUp(self):
        self.service = SimulationService()
        self.app = create_app(service=self.service, preview=True)
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def csrf(self):
        return self.client.get("/api/config").json["csrf_token"]

    def payload(self):
        return {"candidate_mode": "healthy", "session_count": 3, "seed": 42, "idempotency_token": str(uuid4())}

    def test_home_has_controls_and_no_inline_script(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Run simulation', response.data)
        self.assertIn(b'/static/app.js', response.data)
        self.assertNotIn(b'<script>', response.data)
        self.assertIn("script-src 'self'", response.headers["Content-Security-Policy"])
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_javascript_is_served_with_executable_mime_type(self):
        response = self.client.get("/static/app.js")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "text/javascript")
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        response.close()

    def test_post_requires_token_from_the_same_browser_session(self):
        token = self.csrf()
        other = self.app.test_client()
        response = other.post("/api/runs", json=self.payload(), headers={"X-CSRF-Token": token})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.client.post("/api/runs", json=self.payload()).status_code, 403)
        self.assertEqual(len(self.service.list_runs()), 1)

    def test_valid_launch_is_idempotent_with_the_same_request_id(self):
        headers = {"X-CSRF-Token": self.csrf()}
        payload = self.payload()
        first = self.client.post("/api/runs", json=payload, headers=headers)
        second = self.client.post("/api/runs", json=payload, headers=headers)
        self.assertEqual(first.status_code, 202)
        self.assertEqual(first.json, second.json)
        self.assertEqual(len(self.service.list_runs()), 2)
        self.assertEqual(self.service.get_run(first.json["run_id"])["parameters"]["session_count"], 3)

    def test_launch_rejects_non_object_json(self):
        response = self.client.post("/api/runs", json=["healthy"], headers={"X-CSRF-Token": self.csrf()})
        self.assertEqual(response.status_code, 400)

    def test_launch_rejects_invalid_settings_with_a_friendly_error(self):
        payload = self.payload()
        payload["session_count"] = 1001
        response = self.client.post("/api/runs", json=payload, headers={"X-CSRF-Token": self.csrf()})
        self.assertEqual(response.status_code, 400)
        self.assertIn("valid", response.json["error"])
        self.assertEqual(len(self.service.list_runs()), 1)

    def test_unknown_run_never_falls_back_to_another_result(self):
        response = self.client.get("/api/runs/123/results")
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("summary", response.json)

    def test_results_remain_tied_to_the_requested_run(self):
        response = self.client.get("/api/runs/900100/results")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["execution_id"], "preview:900100:0")
        self.assertEqual(response.json["summary"]["session_count"], 100)
        self.assertEqual(response.json["summary"]["passed_checks"], 1300)
        self.assertEqual(response.json["summary"]["failed_checks"], 100)
        self.assertEqual(response.json["summary"]["verdict"], "FAIL")
        self.assertEqual(len(response.json["bills"]), 200)
        self.assertTrue(all(row["execution_id"] == "preview:900100:0" for row in response.json["bills"]))

    def test_preview_running_job_has_no_billing_result(self):
        run = self.service.start_run(self.payload())
        response = self.client.get(f'/api/runs/{run["run_id"]}/results')
        self.assertIsNone(response.json["summary"])
        self.assertEqual(response.json["bills"], [])

    def test_live_cookies_are_secure_httponly_and_same_site(self):
        live = create_app(service=self.service)
        response = live.test_client().get("/api/config")
        cookie = response.headers["Set-Cookie"]
        for flag in ("Secure", "HttpOnly", "SameSite=Lax"):
            self.assertIn(flag, cookie)

    def test_unexpected_errors_do_not_expose_server_details(self):
        broken = Mock()
        broken.get_config.side_effect = RuntimeError("private-token-do-not-show")
        app = create_app(service=broken, preview=True)
        with self.assertLogs(app.logger.name, level="ERROR"):
            response = app.test_client().get("/api/config")
        self.assertEqual(response.status_code, 500)
        self.assertNotIn(b"private-token", response.data)
        self.assertIn("app logs", response.json["error"])

    def test_nonpositive_run_id_is_rejected_before_service_call(self):
        self.assertEqual(self.client.get("/api/runs/0").status_code, 400)
        self.assertEqual(self.client.get("/api/runs/0/results").status_code, 400)

    def test_live_application_creation_does_not_connect_to_databricks(self):
        app = create_app()
        self.assertIsNone(app.extensions["simulation_service"])
        self.assertEqual(app.test_client().get("/").status_code, 200)


if __name__ == "__main__":
    unittest.main()
