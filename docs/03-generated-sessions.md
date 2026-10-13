# Generated sessions: run and check

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

The summary should list `chargeassert_dev_run_generated_pipeline` and `chargeassert_dev_evaluate_generated_batch` with job URLs.

## 2. Run the whole pipeline with one command

Choose the billing mode; the other settings have defaults:

```bash
databricks bundle run -t dev run_generated_pipeline --params candidate_mode=healthy
```

Each new run picks a batch name such as `sessions-604035863697496` from the parent run ID. You can find it in the run's parameters. It defaults to **two sessions and seed `42`**. New batches get new session IDs; the same seed keeps their usage values repeatable.

To create more sessions, add the count:

```bash
databricks bundle run -t dev run_generated_pipeline --params candidate_mode=healthy,session_count=10
```

You can also add `,seed=123` for different usage values. Counts must be 1-1,000 and seeds 0-4,294,967,295. If you omit `candidate_mode`, it defaults to `healthy`.

The parent job runs three existing jobs in order:

1. **Generate:** create the requested number of simulated charging sessions.
2. **Ingest:** Auto Loader loads their events into Bronze (three per session).
3. **Evaluate:** calculate the expected charges, compare both mock billing versions and save the results in Gold.

Each step waits for the previous one to succeed. With the default two sessions, expect **28 PASS checks**: two sessions × two billing versions × seven checks. With ten sessions, expect 140. Baseline, candidate and overall should all be PASS.

The workflow is defined in [generated_billing.yml](../resources/generated_billing.yml). Databricks manages the order and shows all three steps in one parent run. The existing ingestion job keeps ownership of its checkpoint.

**Check that bad billing gets caught.** Create a new batch with a deliberate EUR 0.87 added to each candidate bill:

```bash
databricks bundle run -t dev run_generated_pipeline --params candidate_mode=amount_error
```

Expect **26 PASS and 2 FAIL checks**. Baseline stays PASS; candidate and overall become FAIL. The job itself should succeed because it detected and saved the billing errors.

**Reuse the same sessions.** Set a batch name explicitly, then repeat that command with the same count and seed:

```bash
databricks bundle run -t dev run_generated_pipeline --params batch_id=sessions-repeat-001,candidate_mode=healthy
```

With these defaults, expect 28 PASS checks and a new evaluator execution ID each time, with no extra session records and earlier results preserved. Change only `candidate_mode` to `amount_error` to compare bad billing on those same sessions. Without an explicit batch name, every new full run creates new sessions.

The separate commands still work. `combined_ingestion` creates and loads a batch without evaluating it. Use `evaluate_generated_batch` directly when you only want to check bills for a batch already loaded.

## 3. Read the Gold results

The evaluation job creates `generated_execution_verdict` when it starts. After it finishes, run this in the Databricks SQL editor:

```sql
SELECT execution_id, session_count, assertion_count,
       baseline_verdict, candidate_verdict, verdict
FROM workspace.chargeassert_dev_gold.generated_execution_verdict
ORDER BY captured_at DESC
LIMIT 10;
```

For a parent run, open its `evaluate_generated_batch` task, follow the child run and read the `evaluate_batch` notebook output. Use the `execution_id` printed there; it belongs to the evaluator, not the parent run. With the default count, both modes should show **2 sessions and 28 checks**; their verdicts differ as described above. A custom count produces 14 checks per session.

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

This confirms the saved healthy Gold result from the separate-job flow. The new parent workflow, matching job completion record, deliberate-error test, repeat runs and failure recovery still need checking in Databricks.

See the [table guide](01-tables.md) for what each table does and the [runbook](02-runbook.md#evaluate-generated-batches-through-gold) for detailed rules and limits.
