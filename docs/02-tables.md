# 02 — ChargeAssert table guide

**ChargeAssert uses charging data to catch wrong bills and potentially unbilled sessions before a software release.**

**Bronze keeps the evidence → Silver works out and prepares the charges → Gold explains the billing result.**

There are two separate billing paths. `create_tables` checks the original seven fixed scenarios. `evaluate_generated_batch` checks a selected batch of generated sessions. After both paths and ingestion have run, there are **26 Delta tables**: the original 13 and 13 new `generated_*` tables. The tables share the Bronze, Silver and Gold schemas, but their records stay separate.

The **baseline** is the reference billing behavior; the **candidate** is the behavior under test. Today both are mocks, not real software release builds.

## Bronze: keep the original evidence

| Original table | Generated-batch table | What it does |
| --- | --- | --- |
| `run_manifest` | `generated_run_manifest` | Records the scenario, input identifiers and fingerprints of the billing code and behavior. |
| — | `generated_input_session` | Lists every session that must be checked, so an entire missing session cannot disappear from a smaller PASS result. |
| `ocpp_transaction_events_raw` | `generated_ocpp_transaction_events_raw` | Keeps original charging events, timestamps and meter readings. |
| `tariffs_raw` | `generated_tariffs_raw` | Keeps the source tariff and its validity period. |
| `ocpi_cdrs_raw` | `generated_ocpi_cdrs_raw` | Keeps the baseline and candidate billing records separately. A CDR is a charge detail record. |
| `job_execution` | `generated_job_execution` | Records whether a billing evaluation finished and why. The generated table tracks the evaluation, not the whole simulation. |
| `ocpp_events_landing` | Shared input | Keeps file lines, source details and hashes from Auto Loader. |

The session generator writes three charging events per session. Auto Loader saves them in the landing table. The generated evaluator then validates the selected batch, checks that every declared session is complete, and runs Python mocks to produce both sets of billing records. It does not accept incomplete records from the earlier mixed event/tariff/CDR files; those files stay preserved for investigation.

## Silver: work out what should have been billed

| Original table | Generated-batch table | What it does |
| --- | --- | --- |
| `session_lifecycle` | `generated_session_lifecycle` | Builds each session's start, end and meter readings. |
| `tariff_history` | `generated_tariff_history` | Holds the price and validity period used for the session. |
| `expected_ledger` | `generated_expected_ledger` | Independently calculates expected energy and charge from session readings and the tariff. |
| `actual_ledger` | `generated_actual_ledger` | Puts reported bills into a consistent format, removes equivalent copies and rejects conflicting values. |

The expected calculation uses decimal arithmetic and rounds the session total HALF_UP to cents. It does not read the reported bill. Checking both billing versions against it can catch the same mistake in both versions.

## Gold: give teams a result they can act on

| Original table | Generated-batch table | What it does |
| --- | --- | --- |
| `assertion_result` | `generated_assertion_result` | Shows each check's expected value, reported value, difference and PASS, FAIL or BLOCKED result. |
| `release_verdict` | `generated_release_verdict` | Gives the current billing decision, with counts and a reason. |
| `execution_verdict` | `generated_execution_verdict` | Saves complete checks and a verdict for one exact job attempt. Later runs do not overwrite these snapshots. |

Gold helps billing teams see overcharges and find potentially unbilled sessions. A successful evaluation can still report **FAIL**: the checks ran and found a billing problem.

The [simulation app](06-simulation-app.md) and [dashboard](05-dashboard.md) read two existing tables for generated sessions. Bronze’s `generated_job_execution` tells them whether the evaluation finished. Gold’s `generated_execution_verdict` holds the verdict and the checks behind each bill. They match both records by `execution_id`. If the evidence is missing or inconsistent, they withhold the billing result instead of showing an older PASS.

## What the demos show

The original demo prices **12.5 kWh at EUR 0.45/kWh** as **EUR 5.63**, excluding VAT. A candidate reporting EUR 6.50 fails by EUR 0.87; its corrected response passes. The seven fixed scenarios also include a missing billing record and its correction. They produce 98 checks and seven saved verdict snapshots per successful execution.

Generated sessions have varied meter readings and durations. Each session has seven checks for each billing version: expected calculation available, final-record count, energy, duration, currency, tariff and amount.

| Generated test with 100 sessions | Baseline | Candidate | Checks |
| --- | --- | --- | --- |
| `candidate_mode=healthy` | PASS | PASS | 1,400 PASS |
| `candidate_mode=amount_error` (default full pipeline) | PASS | FAIL | 1,300 PASS / 100 FAIL; each candidate amount is EUR 0.87 too high |

For the same batch, both modes use the same charging inputs and keep separate results. Evaluating that batch again adds a new execution snapshot without adding duplicate session records. The full pipeline creates a new batch by default; set `batch_id` to reuse one.

## Current status and commands

The [generated-session guide](04-generated-sessions.md#confirmed-result) records a healthy two-session result with 28 passing checks. The [dashboard example](05-dashboard.md) shows the other case: evaluation succeeded, but 100 deliberate overcharges failed their checks.

`run_generated_pipeline` generates the sessions, loads their events and checks the bills. Starting it from the app uses the same tables as starting it from the terminal. The separate `setup_simulation_app` job grants read access to the two result tables; it creates no tables.

- [Original MVP brief](01-mvp-brief.pdf) — the business problem and target scope.
- [Full runbook](03-runbook.md) — table rules, field definitions, limitations and remaining work.
- [Deploy the fixed demo](03-runbook.md#billing-regression-job-and-deployment) · [Check its execution](03-runbook.md#check-the-requested-execution).
- [Generate sessions and check Gold](04-generated-sessions.md) · [File-ingestion demo](03-runbook.md#incremental-ocpp-file-ingestion).
