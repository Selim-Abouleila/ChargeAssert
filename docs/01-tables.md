# 01 — ChargeAssert table guide

**ChargeAssert uses charging data to catch wrong bills and potentially unbilled sessions before a software release.**

The project has **13 Delta tables across Bronze, Silver and Gold**, managed in Databricks with Unity Catalog. The billing job uses 12 tables; a separate file-ingestion demo uses the thirteenth.

**Bronze keeps the evidence → Silver calculates and prepares the charges → Gold explains the billing result.**

The **baseline** is the reference billing behavior; the **candidate** is the change being tested.

## Bronze: keep the original evidence

These tables let a team trace a result back to its inputs and the job that produced it.

| Table | What it does |
| --- | --- |
| `run_manifest` | Records each test scenario and the identifiers for its inputs and billing behaviors. |
| `ocpp_transaction_events_raw` | Keeps the original charging events, including timestamps and meter readings. |
| `tariffs_raw` | Stores the source prices and the dates when they apply. |
| `ocpi_cdrs_raw` | Keeps the billing records reported by the baseline and candidate, separately. A CDR is a charge detail record. |
| `job_execution` | Records each Databricks job attempt, whether it finished, and why it failed if it did. |
| `ocpp_events_landing` | Receives original file lines through Auto Loader, with filenames, timestamps and hashes. |

The manual session generator creates varied charging events as files. Auto Loader loads them into `ocpp_events_landing` and remembers processed files between runs. This Bronze data is then automatically parsed and fed into the downstream Silver and Gold billing checks alongside test fixtures. The generator stops after each batch; no automatic schedule is configured.

## Silver: work out what should have been billed

Silver turns the source evidence into records that can be compared.

| Table | What it does |
| --- | --- |
| `session_lifecycle` | Builds a session's start, end and meter readings from charging events. |
| `tariff_history` | Keeps the price and validity period needed to choose the session's tariff. |
| `expected_ledger` | Independently calculates the expected energy and charge from the session and tariff. |
| `actual_ledger` | Puts reported billing records into a consistent format, removing equivalent copies and rejecting conflicts. |

The expected calculation does not use the reported bill. Both billing versions are checked against it, so the same mistake in both versions can still be detected.

## Gold: give teams a result they can act on

Gold helps teams spot customer overcharges, find potentially unbilled sessions and review a release with supporting evidence.

| Table | What it does |
| --- | --- |
| `assertion_result` | Shows each check's expected value, reported value, difference and PASS, FAIL or BLOCKED result. |
| `release_verdict` | Gives the current PASS/FAIL decision for each scenario, with counts and a reason. |
| `execution_verdict` | Saves the verdict and check details for one exact job attempt, without overwriting earlier results. |

A successful job means the checks ran and the evidence was saved. Deliberately faulty billing cases should still show **FAIL**. A missing or failed execution must return **BLOCKED**, rather than reuse an older PASS.

## What the demo shows

For a **12.5 kWh session at EUR 0.45/kWh**, the expected charge is **EUR 5.63**, rounded at the session total, excluding VAT.

| Mock response | Reported amount | Billing result |
| --- | ---: | --- |
| Faulty candidate | EUR 6.50 | FAIL: EUR 0.87 too high |
| Corrected candidate | EUR 5.63 | PASS |

Seven fixed scenarios cover healthy bills, wrong amounts, missing billing records and corrected responses. Each scenario runs seven checks for both billing versions: expected calculation available, record count, energy, duration, currency, tariff and amount.

## What works today and what comes next

This is a working MVP with synthetic data and mock billing responses. The amount examples and an execution containing seven saved scenario snapshots have been verified in Databricks.

Next: verify generated sessions and repeat ingestion in Databricks, test recovery, inspect the missing-record results, and connect incoming events to the billing checks. Testing real software releases and publishing automated GitHub release checks are still planned.

## Details and commands

- [Original MVP brief](OVERVIEW.pdf) — the business problem and target scope.
- [Full runbook](02-runbook.md) — table rules, field definitions, limitations and remaining work.
- [Deploy and run](02-runbook.md#billing-regression-job-and-deployment) · [Check an execution](02-runbook.md#check-the-requested-execution) · [Generate sessions](02-runbook.md#generate-new-charging-sessions) · [File-ingestion demo](02-runbook.md#incremental-ocpp-file-ingestion).
