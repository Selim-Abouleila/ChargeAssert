"""Check bundle wiring; Databricks runs establish runtime failure behavior."""

import json
from pathlib import Path
import re
import unittest

from notebooks import session_generator as generator


ROOT = Path(__file__).resolve().parents[1]
CHILDREN = ("generate_sessions", "ingest_ocpp_files", "evaluate_generated_batch")


class GeneratedWorkflowTests(unittest.TestCase):
    def setUp(self):
        # Match only this repository's resource/task indentation, not general YAML.
        self.jobs = {}
        for path in sorted((ROOT / "resources").glob("*.yml")):
            resource = path.read_text(encoding="utf-8")
            if "\n  jobs:\n" not in resource:
                continue
            for name, job in re.findall(
                r"(?ms)^    (\w+):\n(.*?)(?=^    \w+:\n|\Z)",
                resource.split("\n  jobs:\n", 1)[1],
            ):
                self.assertNotIn(name, self.jobs, "Job resources must have unique keys")
                self.jobs[name] = job
        self.parent = self.jobs["run_generated_pipeline"]
        self.defaults = dict(re.findall(
            r'(?m)^        - name: (\w+)\n          default: "?([^"\n]+)"?$', self.parent,
        ))
        task_pairs = re.findall(
            r"(?ms)^        - task_key: (\w+)\n(.*?)(?=^        - task_key: |\Z)",
            self.parent,
        )
        self.assertEqual([name for name, _ in task_pairs], list(CHILDREN))
        self.tasks = dict(task_pairs)

    def test_manual_workflow_has_defaults_and_serializes_each_job(self):
        self.assertIn("      name: chargeassert_dev_run_generated_pipeline\n", self.parent)
        self.assertEqual(self.defaults, {
            "batch_id": "sessions-{{job.run_id}}", "session_count": "2",
            "seed": "42", "candidate_mode": "healthy",
        })
        for name in ("run_generated_pipeline", *CHILDREN):
            with self.subTest(job=name):
                job = self.jobs[name]
                self.assertEqual(re.findall(r"(?m)^      max_concurrent_runs: (.+)$", job), ["1"])
                self.assertNotRegex(job, r"(?m)^      (?:schedule|trigger|continuous):")

    def test_automatic_batch_names_fit_generator_and_separate_sessions(self):
        session_ids = []
        for run_id in (1, 2**63 - 1):
            with self.subTest(run_id=run_id):
                # Check the producer contract after resolution; Databricks must
                # still establish runtime resolution of the job parameter.
                batch_id = self.defaults["batch_id"].replace("{{job.run_id}}", str(run_id))
                content = generator.generate_batch(
                    batch_id, int(self.defaults["session_count"]), int(self.defaults["seed"]),
                )
                rows = [json.loads(line) for line in content.splitlines()]
                identities = {json.loads(row["payload"])["transactionInfo"]["transactionId"]
                              for row in rows}
                self.assertLessEqual(len(batch_id), 32)
                self.assertEqual(len(identities), 2)
                self.assertEqual({row["generator"]["batch_id"] for row in rows}, {batch_id})
                session_ids.append(identities)
        self.assertEqual(len(session_ids), 2)
        self.assertTrue(session_ids[0].isdisjoint(session_ids[1]))

    def test_named_batch_and_count_override_keep_repeatable_inputs(self):
        parameters = {**self.defaults, "batch_id": "sessions-replay-001", "session_count": "5"}
        arguments = (parameters["batch_id"], int(parameters["session_count"]), int(parameters["seed"]))
        content = generator.generate_batch(*arguments)
        self.assertEqual(content, generator.generate_batch(*arguments))
        rows = [json.loads(line) for line in content.splitlines()]
        identities = {json.loads(row["payload"])["transactionInfo"]["transactionId"]
                      for row in rows}
        self.assertEqual(len(identities), 5)
        self.assertEqual({row["generator"]["seed"] for row in rows}, {42})
        self.assertEqual({row["generator"]["batch_id"] for row in rows}, {"sessions-replay-001"})

    def test_direct_child_jobs_run_in_order_only_after_success(self):
        for index, name in enumerate(CHILDREN):
            with self.subTest(task=name):
                task = self.tasks[name]
                self.assertEqual(re.findall(r"(?m)^          (\w+_task):$", task), ["run_job_task"])
                self.assertEqual(re.findall(r"(?m)^            job_id: (.+)$", task),
                                 ["${resources.jobs." + name + ".id}"])
                dependencies = re.findall(r"(?m)^            - task_key: (\w+)$", task)
                self.assertEqual(dependencies, [CHILDREN[index - 1]] if index else [])
                if index:
                    self.assertIn("          depends_on:\n", task)
                    self.assertEqual(re.findall(r"(?m)^          run_if: (.+)$", task), ["ALL_SUCCESS"])
                else:
                    self.assertNotIn("depends_on:", task)

    def test_generation_and_evaluation_receive_the_same_batch(self):
        expected_parameters = {
            "generate_sessions": ("batch_id", "session_count", "seed"),
            "evaluate_generated_batch": ("batch_id", "candidate_mode"),
        }
        for child, names in expected_parameters.items():
            with self.subTest(child=child):
                task = self.tasks[child]
                self.assertIn("            job_parameters:\n", task)
                forwarded = dict(re.findall(r'(?m)^              (\w+): "(.+)"$', task))
                self.assertEqual(forwarded, {name: "{{job.parameters." + name + "}}" for name in names})
        self.assertNotIn("job_parameters:", self.tasks["ingest_ocpp_files"])

    def test_children_keep_notebooks_checkpoint_and_evaluation_identity(self):
        ownership = {
            "notebook_path: ../notebooks/generate_sessions.py": "generate_sessions",
            "notebook_path: ../notebooks/ingest_ocpp_files.py": "ingest_ocpp_files",
            "checkpoint_path:": "ingest_ocpp_files",
            "notebook_path: ../notebooks/evaluate_generated_batch.py": "evaluate_generated_batch",
        }
        for setting, owner in ownership.items():
            with self.subTest(setting=setting):
                self.assertEqual([name for name, job in self.jobs.items() if setting in job], [owner])
                self.assertEqual(self.jobs[owner].count(setting), 1)
        for child in CHILDREN:
            # Each referenced child is a leaf: there is no path back to the parent.
            self.assertNotIn("run_job_task:", self.jobs[child])
        evaluator = self.jobs["evaluate_generated_batch"]
        for parameter, context in (("job_id", "id"), ("job_run_id", "run_id"), ("repair_count", "repair_count")):
            self.assertIn('              ' + parameter + ': "{{job.' + context + '}}"\n', evaluator)


if __name__ == "__main__":
    unittest.main()
