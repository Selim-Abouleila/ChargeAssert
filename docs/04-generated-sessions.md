# 04 — Generated sessions: run and check

This test creates simulated charging sessions and checks their bills from the incoming files through Gold. It shows whether correct bills pass and incorrect bills are caught.

Each command runs once and stops. Nothing runs continuously.

## 1. Update and deploy

Run from the ChargeAssert folder in the Databricks terminal. Wait for each command to succeed before continuing.

```bash
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

The summary should list `chargeassert_dev_run_generated_pipeline`, `chargeassert_dev_evaluate_generated_batch` and the **ChargeAssert - Generated billing results (dev)** dashboard with URLs. The dashboard is published automatically; no UI setup is needed.

## 2. Run the whole pipeline with one command

Run with the defaults:

```bash
databricks bundle run -t dev run_generated_pipeline
```

Each new run picks a batch name such as `sessions-604035863697496` from the parent run ID. You can find it in the run's parameters. The defaults are **100 sessions, seed `42` and `candidate_mode=amount_error`**. New batches get new session IDs; the same seed keeps their usage values repeatable.

The parent job runs three existing jobs in order:

1. **Generate:** create 100 simulated charging sessions.
2. **Ingest:** Auto Loader loads their 300 events into Bronze (three per session).
3. **Evaluate:** calculate the expected charges, compare both mock billing versions and save the results in Gold.

Each step waits for the previous one to succeed. The default candidate adds EUR 0.87 to every bill. Expect **1,300 PASS and 100 FAIL checks**: 100 sessions × two billing versions × seven checks. Baseline stays PASS; candidate and overall become FAIL. The job itself should succeed because it detected and saved the billing errors.

The workflow is defined in [generated_billing.yml](../resources/generated_billing.yml). Databricks manages the order and shows all three steps in one parent run. The existing ingestion job keeps ownership of its checkpoint.

**Try correct billing instead:**

```bash
databricks bundle run -t dev run_generated_pipeline --params candidate_mode=healthy
```

Expect **1,400 PASS checks** and all three verdicts PASS.

**Choose a smaller batch:**

```bash
databricks bundle run -t dev run_generated_pipeline --params session_count=10
```

This keeps `amount_error`, so expect 130 PASS and 10 FAIL checks. To change both settings, use `--params candidate_mode=healthy,session_count=10` (140 PASS checks). You can also add `,seed=123` for different usage values. Counts must be 1-1,000 and seeds 0-4,294,967,295.

**Reuse the same sessions.** Set a batch name explicitly, then repeat that command with the same count and seed:

```bash
databricks bundle run -t dev run_generated_pipeline --params batch_id=sessions-repeat-001,session_count=2,candidate_mode=healthy
```

With this explicit two-session setting, expect 28 PASS checks and a new evaluator execution ID each time, with no extra session records and earlier results preserved. Change only `candidate_mode` to `amount_error` to compare bad billing on those same sessions. Without an explicit batch name, every new full run creates new sessions.

The separate commands still work. `combined_ingestion` creates and loads a batch without evaluating it. Use `evaluate_generated_batch` directly when you only want to check bills for a batch already loaded.

## 3. Read the results

For normal runs, you can use **Jobs & Pipelines → chargeassert_dev_run_generated_pipeline → Run now**. Its evaluator refreshes the results dashboard after checking the bills. Open **ChargeAssert - Generated billing results (dev)** to inspect the summary and session bills. The [dashboard guide](05-dashboard.md) explains the execution selector and what each status means.

The SQL below remains available for direct checks.

The evaluation job creates `generated_execution_verdict` when it starts. After it finishes, run this in the Databricks SQL editor:

```sql
SELECT execution_id, session_count, assertion_count,
       baseline_verdict, candidate_verdict, verdict
FROM workspace.chargeassert_dev_gold.generated_execution_verdict
ORDER BY captured_at DESC
LIMIT 10;
```

For a parent run, open its `evaluate_generated_batch` task, follow the child run and read the `evaluate_batch` notebook output. Use the `execution_id` printed there; it belongs to the evaluator, not the parent run. With the default count, both modes should show **100 sessions and 1,400 checks**; their verdicts differ as described above. A custom count produces 14 checks per session.

This query shows saved billing results. To also confirm that the matching job completed successfully, use [the exact-execution check](../sql/15_check_generated_billing.sql).

Check that the parent job finished successfully as well. If generation or ingestion fails, there is no new evaluator result; an older Gold row does not prove the failed run passed. Fix the cause and start a new full run. If the Gold table is missing, check whether the evaluator started.

## Confirmed result

The SQL output shared on **2026-10-04** showed:

| Field | Recorded value |
| --- | --- |
| Execution ID | `1101351879096886:604035863697496:0` |
| Sessions | 2 |
| Checks | 28 |
| Baseline / candidate / overall | PASS / PASS / PASS |

This confirms the saved healthy Gold result from the separate-job flow. The parent workflow with the new 100-session default, matching job completion record, deliberate-error test, repeat runs and failure recovery still need checking in Databricks.

See the [table guide](02-tables.md) for what each table does and the [runbook](03-runbook.md#evaluate-generated-batches-through-gold) for detailed rules and limits.
