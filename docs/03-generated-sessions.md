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

The summary should list `chargeassert_dev_evaluate_generated_batch` with a job URL.

## 2. Create sessions and check billing

Use a fresh batch name and keep it the same in all three commands below.

**Create and load two sessions.** Auto Loader loads their six charging events into Bronze. `seed=42` makes the data repeatable.

```bash
databricks bundle run -t dev combined_ingestion --params batch_id=sessions-check-001,session_count=2,seed=42
```

**Check correct billing.** The evaluator calculates the expected charges, compares both simulated billing versions and saves the results in Gold.

```bash
databricks bundle run -t dev evaluate_generated_batch --params batch_id=sessions-check-001,candidate_mode=healthy
```

Expect **28 PASS checks**: two sessions × two billing versions × seven checks. Baseline, candidate and overall should all be PASS.

**Check that bad billing gets caught.** This uses the same sessions but adds EUR 0.87 to each candidate bill.

```bash
databricks bundle run -t dev evaluate_generated_batch --params batch_id=sessions-check-001,candidate_mode=amount_error
```

Expect **26 PASS and 2 FAIL checks**. Baseline stays PASS; candidate and overall become FAIL. The job itself should succeed because it detected and saved the billing errors.

Run the healthy command again to check repeat runs. Expect 28 PASS checks and a new execution ID, with no extra session records and the earlier results preserved.

## 3. Read the Gold results

The evaluation job creates `generated_execution_verdict` when it starts. After it finishes, run this in the Databricks SQL editor:

```sql
SELECT execution_id, session_count, assertion_count,
       baseline_verdict, candidate_verdict, verdict
FROM workspace.chargeassert_dev_gold.generated_execution_verdict
ORDER BY captured_at DESC
LIMIT 10;
```

Match `execution_id` to the one printed by your command. Both modes should show **2 sessions and 28 checks**; their verdicts differ as described above.

This query shows saved billing results. To also confirm that the matching job completed successfully, use [the exact-execution check](../sql/15_check_generated_billing.sql).

If a command fails, stop there and inspect its error. If the Gold table is missing, check whether `evaluate_generated_batch` started successfully.

## Confirmed result

The SQL output shared on **2026-10-04** showed:

| Field | Recorded value |
| --- | --- |
| Execution ID | `1101351879096886:604035863697496:0` |
| Sessions | 2 |
| Checks | 28 |
| Baseline / candidate / overall | PASS / PASS / PASS |

This confirms the saved healthy Gold result. The matching job completion record, deliberate-error test, repeat runs and failure recovery still need checking in Databricks.

See the [table guide](01-tables.md) for what each table does and the [runbook](02-runbook.md#evaluate-generated-batches-through-gold) for detailed rules and limits.
