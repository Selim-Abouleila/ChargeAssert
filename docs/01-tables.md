# 01 — MVP tables and remaining work

The product scope is defined in [OVERVIEW.pdf](OVERVIEW.pdf). The MVP replays the same deterministic scenario against a baseline and a candidate, compares both with an independent financial calculation, and produces an explainable GitHub PASS/FAIL verdict. A deliberately faulty candidate must fail; its corrected version must pass the exact same manifest.

## Current implementation status

ChargeAssert uses managed Delta tables inside these Unity Catalog schemas:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

Twelve tables now have version-controlled SQL or Python creation code wired into the `create_tables` job, which has fifteen tasks including fixture loading, an on-demand Python mock billing generator and execution tracking. **Implemented means code exists, not that its latest version has been deployed or successfully run.** The user confirmed all 14 Gold smoke assertions PASS on 2026-09-18, the smoke release verdict PASS with 14/14 checks on 2026-09-19, and the paired canned fixtures on 2026-09-20. The Python-generated amount pair was confirmed in Databricks on 2026-09-21: `mock-amount-bad-v1` has baseline PASS, candidate/overall FAIL and 13 passing / 1 failing assertions; `mock-amount-fixed-v1` has both releases/overall PASS and 14 passing / 0 failing assertions. Execution tracking and the new missing-CDR pair are implemented but still need deployment and workspace verification.

The job retains three canned regression runs (`smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`) and four runs with computed mock behavior (`mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1`, `mock-missing-cdr-fixed-v1`). Each has one session (`txn-smoke-v1`), the same three OCPP-shaped input events and one EUR energy tariff. The Python mock calculates the reported charge from the raw meter events and tariff, while the independent Silver SQL oracle calculates the expectation separately. The bad amount candidates report EUR 6.50; healthy responses report EUR 5.63. The bad missing-CDR candidate returns no billing record at all. Every other run has one baseline and one candidate CDR. This remains a small synthetic demonstration, not the complete release gate or six-scenario suite.

The generator runs once when the job is started and then exits. There is no schedule, continuous loop, HTTP server or background service.

A scenario `run_id` identifies retained test evidence; a new `execution_id` identifies one Databricks job attempt evaluating it. Every complete job invocation creates its own audit record and verdict snapshots. Query the requested execution explicitly: an old `release_verdict` PASS is diagnostic data and cannot stand in for a failed or incomplete new execution.

## Bronze — preserve the evidence

Bronze preserves original inputs and the captured baseline and candidate outputs.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `run_manifest` | Implemented | One deterministic test run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `job_execution` | Implemented; workspace verification pending | One Databricks job attempt and its completion state | `execution_id`, Databricks job/run identifiers, repair count, `status`, `expected_run_ids_json`, start/finish timestamps, task states, `reason` |
| `ocpp_transaction_events_raw` | Implemented | One received OCPP 2.0.1-shaped `TransactionEvent` | `run_id`, `event_id`, `charging_station_id`, `transaction_id`, `event_type`, `sequence_number`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpi_cdrs_raw` | Implemented with canned fixtures and computed mock responses | One distinct CDR payload captured for one release in one run | `run_id`, `release_role`, `country_code`, `party_id`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `payload_hash`, `ingest_time` |
| `tariffs_raw` | Implemented | One input tariff version in a run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `payload`, `payload_hash` |

`release_role` (`baseline` or `candidate`) now isolates the raw CDR outputs. The two smoke responses intentionally use the same CDR ID and payload to verify that both releases retain their own evidence. The current manifest's two SHA columns alone would not provide output isolation.

### `job_execution` contract

- Destination: `workspace.chargeassert_dev_bronze.job_execution`; logical key: `execution_id`. This operational audit accompanies the original Bronze evidence; it is not a charging-session record.
- `execution_id` is `<job_id>:<job_run_id>:<repair_count>`, using Databricks job context. A full new job run gets a new identifier even when all seven scenario run IDs and input bytes are unchanged. Invalid or unresolved context is rejected.
- The begin task registers `RUNNING` before the existing pipeline and freezes the seven required scenario IDs in `expected_run_ids_json`: `smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`, `mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1` and `mock-missing-cdr-fixed-v1`. This is a fixed fixture inventory, not yet a general manifest of every intended session.
- The finish task records `SUCCEEDED` only after every required preceding task explicitly succeeded and seven complete, unique snapshots exist for that execution. Otherwise it records `FAILED` with task-state evidence and a reason, then raises an error so the job reports failure. The audit status confirms upstream completion and committed snapshots; confirm the finalizer and the Databricks job itself also finish successfully before accepting a result. Intentional financial `FAIL` and dependent `BLOCKED` assertions are valid captured test results; they do not by themselves mean the job failed operationally.
- A canceled job or unavailable compute can prevent finalization. Such an execution can remain `RUNNING`; if registration never happened, the requested identifier is absent. The check query labels these incomplete/missing outcomes and returns `BLOCKED`, never a previous PASS.
- Repair attempts fail closed in this version. Start a new full job after correcting a failure; partial repairs can reuse successful tasks from an earlier attempt and do not establish complete fresh processing. The job permits only one concurrent run.
- Existing completed executions with the earlier five-run inventory and their immutable snapshots remain unchanged and readable by exact execution ID. The seven-run inventory applies to new full jobs. Do not repair an old attempt to add the new scenarios.

The tracking code is `notebooks/execution_tracking.py`, called by `notebooks/track_execution.py` in begin, capture and finish modes. Context and task outcomes use documented [Databricks dynamic references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references). The finalizer uses [All done dependencies](https://docs.databricks.com/gcp/en/jobs/run-if); it cannot guarantee a completion write if it is itself canceled or fails.

### `ocpi_cdrs_raw` contract

- Destination: `workspace.chargeassert_dev_bronze.ocpi_cdrs_raw`. The original smoke loader depends only on `run_manifest`; the amount-fixture loader reads Bronze inputs and responses. The Python generator reads Bronze event and tariff inputs to compute new responses. All keep reported outputs independent of the expected-charge calculation.
- The first loader seeds two canned responses for `smoke-run-v1`: `baseline` and `candidate`, both reporting `cdr-smoke-v1` for `txn-smoke-v1`, 12.5 kWh, one hour and EUR 5.63. Neither response is read from `expected_ledger`.
- The financial subset uses [OCPI 2.2.1 CDR field names](https://github.com/ocpi/ocpi/blob/release-2.2.1-bugfixes/mod_cdrs.asciidoc). `total_energy` is in kWh, `total_time` is in hours and `total_cost` is a Price object. The fixture omits full token/location data and does not claim protocol certification.
- Preserve the exact JSON body and SHA-256 hash. The insert-only merge key is `(run_id, release_role, payload_hash)`; an exact retry leaves the first capture unchanged. This is a logical key, not a database-enforced uniqueness constraint.
- Different CDR IDs for the same session remain separate because their payloads differ. Changed payloads with the same CDR ID are also retained, so Silver can report a conflicting financial record instead of silently replacing it. Changes in JSON formatting also remain separate raw payloads; semantic deduplication belongs in Silver.
- `country_code`, `party_id` and `cdr_id` preserve the reported record identity. Silver `actual_ledger` uses those owner fields as well as the CDR ID and release/run identity.
- Extracted business fields are nullable so unparseable or incomplete reported values can remain in the raw payload for later validation. `total_cost` is a convenience `DECIMAL(18,6)` projection of `payload.total_cost.excl_vat`, with no cent rounding; the payload retains the original precision. VAT-inclusive comparison is outside this fixture's contract.
- `cdr_type` is ChargeAssert metadata derived from the OCPI `credit` flag: `FINAL` for false/absent, `CREDIT` for true, and null for an invalid flag. Credit handling is not implemented in the billing comparison yet.
- The smoke assertion checks exactly two intact, healthy responses and their extracted values. Separate amount runs preserve the faulty response without changing this healthy fixture. Bronze checks integrity and expected fixture contents; Gold decides financial correctness.
- Still missing: external release/HTTP replay, ingestion of external response files, and delivery-attempt/event traces. Silver validation/deduplication and a small in-process billing mock are implemented for the subset below. Rerunning the loaders or generator checks stored records; it does not exercise a real HTTP retry.

### Paired amount fixtures

`sql/11_seed_amount_scenarios.sql` runs after the original Bronze loaders and before the three Silver transformations. It adds data to the existing tables; it creates no new table.

| Run | Baseline response | Candidate response | Independent expectation | Expected verdict |
| --- | --- | --- | --- | --- |
| `smoke-run-v1` | EUR 5.63 | EUR 5.63 | EUR 5.63 | PASS |
| `amount-bad-v1` | EUR 5.63 | EUR 6.50 | EUR 5.63 | FAIL |
| `amount-fixed-v1` | EUR 5.63 | EUR 5.63 | EUR 5.63 | PASS |

- Both amount manifests use `scenario_id = 'amount-mismatch-v1'`, seed 42, the same fixed timestamps and the same baseline identifier. The OCPP event bytes, tariff bytes, logical event/session/CDR IDs and input tariff hash are identical between the paired runs. Run IDs isolate the evidence.
- Candidate identifiers are deterministic synthetic labels: `sha1('candidate-amount-bad-v1')` and `sha1('candidate-amount-fixed-v1')`. They are not actual tested Git commits. The pair demonstrates expected behavior with canned outputs, not a real code fix or a mock replay.
- The corrected response uses a separate run ID so the original EUR 6.50 evidence and FAIL verdict remain queryable. These are two physical run manifests sharing the same scenario inputs; they are not an in-place rewrite of one manifest.
- New manifests hash the actual tariff payload with SHA-256. The original smoke manifest remains unchanged and still hashes the tariff identifier; full input-snapshot provenance is future work.
- The seed task reads only Bronze evidence. It copies the healthy responses and replaces exactly one fixed amount literal in the bad candidate body, then recomputes its `total_cost` projection and `payload_hash`. It never copies a value from `expected_ledger` or writes a Gold outcome directly.
- Four insert-only merges preserve the existing Bronze keys. Source checks and per-run postchecks reject missing, duplicate or conflicting fixture rows. Repeated execution keeps one manifest, three events, one tariff and two raw CDRs per run. Fixing a fixture definition requires a new version/run ID, not silently overwriting old evidence.
- The expected ledger independently recalculates these sessions from meter differences and the selected tariff: 12.5 × 0.45 = 5.625, rounded HALF_UP to 5.63. The only failed check in the bad run must be candidate `amount_match`, with **actual minus expected = +0.870000**. Its baseline passes; the corrected run has 14 passing checks.
- The final verdict task asserts these expected outcomes. **A successful demo job includes intentional financial FAIL verdicts.** The job itself fails if a bad case unexpectedly passes, a fixed case fails, or fixture coverage is incomplete. The canned pair's 13/1 and 14/0 outcomes were confirmed in Databricks on 2026-09-20.

### On-demand mock billing

`notebooks/mock_billing.py` implements the original amount billing mock using only the Python standard library. `notebooks/mock_missing_cdr.py` adds the missing-CDR behavior and `generate_all_mock_runs` combines the two pairs. `notebooks/generate_mock_billing.py` runs that combined generator in the existing `generate_mock_billing` notebook task after `seed_amount_scenarios`, then writes the evidence to the existing Bronze tables. No new table or task is required.

| Run | Baseline behavior | Candidate behavior | Expected candidate amount | Expected verdict |
| --- | --- | --- | --- | --- |
| `mock-amount-bad-v1` | Healthy calculation | Deliberate EUR 0.87 surcharge | EUR 6.50 | FAIL |
| `mock-amount-fixed-v1` | Healthy calculation | Healthy calculation | EUR 5.63 | PASS |

- For the amount pair, the mock reads the same raw smoke event and tariff JSON for both releases and both runs. It derives energy from the meter difference, duration from the event timestamps and the energy rate from the tariff. It calculates with `Decimal` and rounds the session amount HALF_UP to cents. It does not read Silver, Gold or a canned CDR to obtain the result.
- The faulty behavior adds EUR 0.87 to the calculated amount; the healthy behavior returns the calculated amount. Both generate the supported OCPI-shaped financial subset with `cdr_id = 'cdr-mock-v1'`. Full token/location data and protocol certification remain outside the implementation.
- The source event/tariff bytes, logical input IDs and fixed clock are preserved. New run IDs isolate generated evidence from the old canned fixtures, so their raw payloads and verdicts remain queryable. These fixed scenarios do not yet generate different events from the seed.
- The amount pair's manifests hash the source tariff payload. Their `baseline_sha` and `candidate_sha` fingerprint the original Python billing module bytes together with the selected behavior; these values identify the mock implementation/behavior, not tested Git release commits. The bad and fixed amount runs share the healthy baseline fingerprint and have different candidate fingerprints.
- Insert-only merges and drift checks retain the first evidence and reject conflicting reuse of a run ID. For the amount pair, identical reruns keep one manifest, three events, one tariff and two raw CDRs per run. Changed mock code or inputs require new versioned run IDs and corresponding fixture expectations; do not rewrite existing evidence to make a rerun pass.
- Silver independently prices all seven mapped sessions, including the missing-CDR session for which the candidate returns nothing. The generated bad amount candidate must fail only `amount_match` at +EUR 0.87; its baseline passes, and the generated fixed amount run has 14 passing checks. Gold decides the financial outcome; the generator never writes a PASS/FAIL decision.
- This is one batch task per manual job execution. It requires Databricks serverless notebook/job compute in addition to the existing SQL warehouse. No extra Python packages, scheduled trigger or continuously running process are added. See the [Databricks serverless job bundle example](https://docs.databricks.com/aws/en/dev-tools/bundles/examples#job-that-uses-serverless-compute).
- The user confirmed both generated verdicts and their 13/1 versus 14/0 assertion counts in Databricks on 2026-09-21. Execution tracking is a subsequent change and still needs its own workspace verification.

### Missing-CDR mock scenario

This scenario tests a completed charging session whose final billing record is absent. `notebooks/mock_missing_cdr.py` reuses the original mock's input validation and healthy billing calculation, then deliberately suppresses the faulty candidate's CDR. The raw events and tariff remain present for both releases, so the independent oracle still expects 12.5 kWh, one hour and EUR 5.63.

| Run | Baseline CDRs | Candidate CDRs | Baseline / candidate / overall verdict | Passed | Failed | Blocked |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| `mock-missing-cdr-bad-v1` | 1 | 0 | PASS / FAIL / FAIL | 8 | 1 | 5 |
| `mock-missing-cdr-fixed-v1` | 1 | 1 | PASS / PASS / PASS | 14 | 0 | 0 |

- Both runs use `scenario_id = 'mock-missing-cdr-v1'`, seed 42, session `txn-smoke-v1` and tariff `tariff-smoke-v1`, with identical input bytes and a fixed clock. Separate run IDs retain both faulty and corrected evidence.
- There is no candidate Bronze CDR or Silver actual-ledger row in the bad run. The generator inserts neither a null placeholder nor a made-up zero charge. It checks that no unexpected candidate record already exists under this run ID.
- The bad candidate's `oracle_available` passes. Its `final_cdr_count` fails with **expected 1, actual 0, difference -1**. `energy_match`, `duration_match`, `currency_match`, `tariff_match` and `amount_match` are `BLOCKED` because there is no reported record to compare. The expected amount remains EUR 5.63; the amount's actual value and difference are null.
- The seven baseline checks pass in both runs. In the corrected run, the candidate returns one healthy CDR and all fourteen checks pass. Existing Gold rule logic decides these outcomes; additional fixture checks enforce the expected demonstration results.
- `mock_billing.py` remains byte-for-byte unchanged because existing amount manifests fingerprint its source. Missing-CDR manifest fingerprints include both that helper's source and the new module's source, normalized to LF line endings, plus the selected behavior (`healthy-v1` or `drop-final-cdr-v1`). They identify this mock implementation, not an executed Git release build. A changed source or input requires new versioned run IDs, not overwriting retained evidence.
- The missing record demonstrates a **potentially unbilled EUR 5.63 synthetic session**. It does not prove actual revenue loss, and aggregate leakage or modeled exposure is not calculated yet. This is an in-process mock; HTTP delivery/retry traces are not implemented.
- These two new cases and their execution snapshots still require Databricks verification. Rerunning identical code and inputs must preserve the one-record bad run and two-record fixed run.

## Silver — produce trusted business records

Silver validates, deduplicates and normalizes the raw evidence.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `session_lifecycle` | Implemented, happy path only | One logical charging session in one run | `run_id`, `session_id`, `started_at`, `ended_at`, `meter_start_wh`, `meter_end_wh`, `status` |
| `tariff_history` | Implemented; smoke join verified in Databricks | One effective tariff period in one run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `price_components`, `source_payload_hash` |
| `expected_ledger` | Implemented for seven fixture sessions; original five checked through Gold in Databricks, missing-CDR pair pending | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `price_per_kwh`, `expected_amount_unrounded`, `expected_amount`, `currency`, `tariff_id`, `tariff_valid_from`, `tariff_payload_hash` |
| `actual_ledger` | Implemented for the final-CDR subset; Gold smoke checks verified | One normalized CDR returned by either release | `run_id`, `release_role`, `country_code`, `party_id`, `session_id`, `cdr_id`, `cdr_type`, `started_at`, `ended_at`, `actual_energy_kwh`, `actual_duration_hours`, `actual_amount`, `currency`, `tariff_id`, `source_payload_hashes` |

`actual_ledger` preserves different CDR IDs for the same session and release. Repeated delivery of the same payload is deduplicated; two different financial records for one billable session are preserved and reported as a business defect by Gold.

### `tariff_history` contract

- Source: `workspace.chargeassert_dev_bronze.tariffs_raw`.
- Destination: `workspace.chargeassert_dev_silver.tariff_history`.
- Logical key: `(run_id, tariff_id, valid_from)`. Reusing a tariff ID in another run does not mix snapshots.
- Effective periods use `[valid_from, valid_to)`: inclusive start, exclusive end; a null end is open-ended. Adjacent periods are allowed. Conflicting or overlapping periods for the same run and tariff fail the task.
- `price_components` is `ARRAY<STRUCT<type: STRING, price: DECIMAL(18,6), step_size: INT>>`. The price is stored as a decimal, not a floating-point number.
- The first supported tariff is one unrestricted `ENERGY` component in EUR, with a nonnegative price and `step_size = 1`. VAT, price bounds, additional components/elements and restrictions are rejected. Dates come from the validity columns; payload-level `start_date_time`/`end_date_time` are rejected until their reconciliation is implemented.
- Malformed payloads, missing required values, invalid validity periods, metadata/payload disagreement and invalid payload hashes fail validation. Exact duplicate evidence is deduplicated before merging.
- The source payload hash is retained for traceability. The merge updates the same logical period on rerun rather than inserting another copy; Bronze remains the source of truth.
- The smoke assertion expects one `tariff-smoke-v1` period at `0.450000 EUR/kWh`, with matching Bronze provenance. This transformation does not calculate a session charge or decide billing rounding.

Silver can represent multiple periods for a tariff. The current Bronze fixture merge matches only `(run_id, tariff_id)`; ingestion still needs a version/period key before it can load multiple periods for the same tariff ID in one run.

### `expected_ledger` contract

- Sources: Silver `session_lifecycle` and `tariff_history`. The calculation does not read baseline or candidate billing outputs.
- Destination: `workspace.chargeassert_dev_silver.expected_ledger`; logical key: `(run_id, session_id)`.
- The explicit mapping prices `txn-smoke-v1` against `tariff-smoke-v1` independently in `smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`, `mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1` and `mock-missing-cdr-fixed-v1`. Each required session and tariff match is checked separately, so a missing run cannot be hidden by duplicate rows in another run. The absence of a candidate CDR does not remove its expected charge. Other sessions are outside this fixture mapping; a general scenario registry is still needed.
- Require one completed session with nonnegative, nondecreasing meter readings and valid start/end timestamps, plus exactly one matching tariff effective at the session start. Missing or ambiguous matches fail the task.
- The supported tariff is one EUR ENERGY component with `step_size = 1`. The full session must fit within the selected period; sessions crossing a tariff boundary fail until boundary pricing is implemented.
- Energy is `(meter_end_wh - meter_start_wh) / 1000`, stored as `DECIMAL(18,6)`. Price is `DECIMAL(18,6)` and their unrounded product is retained as `DECIMAL(37,12)`.
- Round the session total once using **HALF_UP to two decimal places**; store `expected_amount` as `DECIMAL(18,2)`. This is the chosen synthetic MVP contract, not a claim about every operator's billing rules. Databricks [`round(amount, 2)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/round) uses HALF_UP.
- Retain the selected tariff ID, period start, payload hash, currency and rate for traceability. Reruns update the same logical ledger row.
- The fixture assertion requires seven independently calculated rows, each with 12.5 kWh at EUR 0.45/kWh, EUR 5.625 unrounded and **EUR 5.63 rounded**. Meter normalization/reset handling still depends on future session validation work.

### `actual_ledger` contract

- Source: Bronze `ocpi_cdrs_raw`; destination: `workspace.chargeassert_dev_silver.actual_ledger`. The task waits for raw CDR loading, the amount fixtures and the mock generator. Its transformation reads only raw CDRs; it does not join to the expected ledger, session evidence or input tariffs to replace reported values.
- Logical key: `(run_id, release_role, country_code, party_id, cdr_id)`. Owner country/party are uppercased and CDR/tariff IDs lowercased for case-insensitive identity; the session ID is preserved for the existing OCPP mapping.
- Parse original JSON with a typed schema. Validate payload integrity, run/release identity, owner/CDR/session IDs, timestamps, currency shape and required nonnegative numeric fields. Invalid or unsupported records fail the task while their evidence remains in Bronze.
- First supported subset: final/non-credit CDRs with one charging period naming a tariff, and plain decimal energy, duration and exclusive-VAT amount with at most six fractional digits. More precision, scientific notation, credit CDRs and multiple charging periods require explicit extensions; they are rejected rather than silently rounded or partially normalized.
- `actual_energy_kwh`, `actual_duration_hours` and `actual_amount` are `DECIMAL(18,6)`, taken from `total_energy`, `total_time` and `total_cost.excl_vat`. A reported EUR 5.625 stays 5.625; this table applies no currency rounding. Reported duration is retained separately from the timestamps.
- Different CDR IDs for one session remain separate. Equivalent normalized billing fields under the same record identity collapse into one row with all distinct `source_payload_hashes` retained in sorted order. Unmodeled payload fields remain available through those hashes.
- Conflicting normalized billing fields for one record identity raise an error naming that run/release/owner/CDR. No arbitrary version is selected; all raw versions remain available. Gold reporting for this failure is still to be implemented.
- A reported amount, energy, duration, currency or tariff can disagree with the independent expectation and still normalize successfully. Comparing those values belongs in Gold. The fixed smoke assertion separately verifies two healthy rows (baseline/candidate), each reporting 12.5 kWh, one hour and EUR 5.63.
- Reruns merge by the logical CDR identity. Both ledgers assume retained source evidence; source deletion/retraction handling and full protocol validation remain outside this first implementation.

Local regression checks are available with:

```powershell
python -B -m unittest discover -s tests -v
```

They exercise production fixture-source, lifecycle, expected-charge, normalization, assertion and verdict SELECT queries with SQLite adapters for parsed fields, timestamps, arrays/JSON and HALF_UP rounding, plus job wiring. They cover release/run isolation, exact and equivalent deliveries, distinct CDR preservation, conflicts, invalid inputs, fractional-cent retention, missing/duplicate CDR assertions, financial mismatches, oracle gaps and evidence. Paired-fixture checks follow the seeded responses through the independent expected calculation and Gold decisions, verify unchanged inputs, reject missing/ambiguous oracle inputs and check repeat loading with an emulation of insert-only keys. Python mock checks exercise the executable calculation and generated outputs independently of Databricks. Verdict tests also cover missing entire releases/sessions, duplicate checks replacing missing checks, unsupported rules/roles/statuses, empty runs and missing/duplicate manifests. These checks do **not** execute a Databricks notebook, `from_json`, Spark type analysis/decimal arithmetic, Delta DDL or actual `MERGE`; workspace execution remains required.

Execution tests exercise the Python publication/completion checks and the canonical SQL consumer against the seven-run financial fixtures, including the missing-CDR run's valid FAIL/BLOCKED assertion snapshot. They cover success followed by pre-Gold failure and a fresh success, stale verdicts, immutable snapshot conflicts, missing/duplicate evidence, incomplete task states, rejected repairs and inconsistent PASS counts. The earlier five-run completed snapshots remain readable by their exact execution ID. Databricks dynamic-reference resolution, scheduler behavior and the actual Spark/Delta writes still require the workspace demonstration below.

## Gold — make the release decision

Gold stores explainable checks, current scenario verdicts and immutable execution snapshots. The GitHub integration and a dashboard are future work.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `assertion_result` | Implemented; 14 smoke PASS rows verified in Databricks | One rule checked for one session and release | `run_id`, `release_role`, `session_id`, `assertion_id`, `expected_value`, `actual_value`, `difference`, `status`, `severity`, `message`, `evidence` |
| `release_verdict` | Implemented for seven fixtures; original five outcomes verified in Databricks, missing-CDR pair pending | The current diagnostic decision for one scenario run, including both release outcomes | `run_id`, manifest provenance, assertion/coverage counts, `baseline_verdict`, `candidate_verdict`, `verdict`, `reason`, `first_problem`, `evidence`, `evaluated_at` |
| `execution_verdict` | Implemented; workspace verification pending | An immutable scenario verdict and assertion snapshot from one job execution | `execution_id`, `run_id`, release verdicts, assertion counts, `reason`, `verdict_snapshot`, `assertions_snapshot` |

The future GitHub check must require a successful current full Databricks job, a unique `job_execution.status = 'SUCCEEDED'` for that exact attempt and a matching `execution_verdict.verdict = 'PASS'` for the requested scenario. A failed/incomplete job, missing or duplicate registration/snapshot, `BLOCKED` check or financial `FAIL` must block the release. Do not fall back to the most recent successful execution or the mutable `release_verdict` table. Direct baseline/candidate differences still need reporting alongside the independent assertions; a shared billing defect already fails both releases against the oracle.

### `assertion_result` contract

- Sources: Silver `expected_ledger`, `actual_ledger` and `session_lifecycle`. Destination: `workspace.chargeassert_dev_gold.assertion_result`.
- Logical key: `(run_id, release_role, session_id, assertion_id)`. Both releases are independently checked against the oracle. A matching mistake in baseline and candidate fails both; the baseline never supplies the expectation.
- Every expected session and every completed lifecycle session produces checks for both releases, including a release with no CDR. Actual-only sessions also produce checks for the reporting release; missing expectations are failures rather than rows silently lost in a join.
- There are **seven rules per session/release**, listed below. `status` is `PASS`, `FAIL` or `BLOCKED`; all current rules have severity `ERROR`. `BLOCKED` means a prerequisite failed, never a pass. `release_verdict` rejects failed/blocked checks and missing required coverage, including a run with zero assertions.
- `expected_value` and `actual_value` are strings so numeric and textual checks share a schema. Numeric comparisons use decimals before string conversion. `difference` is `DECIMAL(38,6)`, calculated as **actual minus expected** only for comparable numeric values. Text checks and blocked checks have a null difference. Amount differences require matching currencies; these are per-check differences, not aggregate leakage or exposure estimates.
- Compare energy and amount exactly at their stored precision. Do not round the reported amount: expected EUR 5.63 versus reported EUR 5.625 fails with a difference of -0.005000. The oracle already applies the configured cent rounding.
- Expected duration is elapsed lifecycle time, calculated with [`timestampdiff(MICROSECOND, started_at, ended_at)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/timestampdiff), divided by 3,600,000,000 using decimals and rounded HALF_UP to six decimal hours. Compare the reported `actual_duration_hours` exactly against that value. This is the synthetic fixture's duration contract; it does not yet distinguish charging time from pauses/parking.
- A missing or duplicate CDR fails `final_cdr_count` and blocks the five value comparisons. No CDR is arbitrarily selected and duplicate amounts are never summed to hide the defect. Missing, duplicate or invalid oracle/lifecycle rows fail `oracle_available` and block dependent checks.
- `evidence` is JSON with rule version, source row counts, selected tariff ID/period/hash/currency, session timestamps, and a sorted list of all actual CDR identities, reported values and Bronze payload hashes. Use the result's run/release/session keys to locate Silver records, and `(run_id, release_role, payload_hash)` to retrieve original Bronze CDRs.
- The [Delta merge](https://docs.databricks.com/gcp/en/delta/merge) updates existing keys, inserts new keys and removes Gold results absent from the complete current source set. This is a derived snapshot over all Silver inputs, not an immutable history of evaluations. Repeating identical inputs produces the same logical rows. Bronze and Silver are not modified by this task; their existing source-deletion limitations still apply. Run-scoped incremental evaluation is future work.
- The fixed healthy fixture must produce **14 PASS rows: seven baseline and seven candidate**. A financial FAIL/BLOCKED in another run is stored without raising a SQL exception. A successful table-creation job is not a passing release verdict. Do not mutate the healthy smoke run to inject faults: upstream fixture assertions intentionally require its original values.

| `assertion_id` | What it checks |
| --- | --- |
| `oracle_available` | Exactly one expected ledger row and one valid completed lifecycle row are available. |
| `final_cdr_count` | Exactly one final financial CDR exists for the completed billable session; zero means missing, more than one means duplicate. |
| `energy_match` | Reported kWh equals independently expected kWh. |
| `duration_match` | Reported duration equals elapsed lifecycle hours rounded to six decimals. |
| `currency_match` | Reported currency equals the oracle currency. |
| `tariff_match` | Reported tariff ID matches the selected tariff ID after case normalization. |
| `amount_match` | Reported exclusive-VAT amount equals the rounded oracle amount in the same currency. |

This first implementation consumes the supported final-CDR Silver contract. It does not yet compare releases directly, verify the full reported tariff version/content, compare reported start/end timestamps, report Silver parsing/conflict failures as Gold rows, or assert HTTP retry/late-event traces. The expected-ledger producer prices the seven explicitly mapped fixture sessions; general scenarios need their own independent expectations. The GitHub gate remains unimplemented.

### `release_verdict` contract

- Sources: Bronze `run_manifest`, Gold `assertion_result`, and Silver `expected_ledger`, `session_lifecycle` and `actual_ledger`. Destination: `workspace.chargeassert_dev_gold.release_verdict`; logical key: `run_id`.
- Produce one row for every run found in any of those tables. A manifest with no sessions/checks gets FAIL; data with no manifest also gets FAIL. Exactly one manifest is required. Copy `scenario_id`, `seed`, `baseline_sha`, `candidate_sha` and `tariff_hash` only when the manifest is unique; missing/duplicate manifests leave this provenance null. Canned-fixture SHAs remain synthetic labels; mock SHAs fingerprint the applicable source modules and behavior. Neither is a validated source-release commit.
- Derive required assertion keys independently from Silver using the same session scope as `assertion_result`: expected-ledger and completed-lifecycle sessions require both release roles; supported FINAL actual-only sessions require their reporting role. Cross those keys with the explicit seven-rule registry. An entire missing session/release cannot disappear by reducing the observed assertion count.
- Each release needs a nonempty required set, exactly one PASS row per required key, and no failed, blocked, duplicate, unexpected or invalid-status assertions. Both `baseline_verdict` and `candidate_verdict` must be PASS for the overall `verdict` to pass. Unknown-role assertions also fail the overall verdict even when the two known releases independently pass.
- Verdicts are `PASS` or `FAIL`. Blocked assertions are retained in the counts but produce a FAIL verdict. A failed baseline blocks the overall run even if the candidate passes; matching baseline/candidate mistakes do not establish correctness.
- `reason` explains the highest-priority problem. `first_problem` is a JSON reference containing problem type and available run/release/session/assertion keys. Order is deterministic: manifest error, empty coverage, missing check, duplicate check, unexpected check, invalid status, FAIL, then BLOCKED; ties sort by release/session/assertion. This is an investigation entry point, **not the first chronological divergence**. For existing checks, retrieve `assertion_result` by these keys to inspect values, messages and original evidence.
- `evidence` contains rule version, manifest-row count and separate `baseline`/`candidate` summaries with coverage counts and verdicts. Run-level totals also include assertions with unsupported release roles.
- The merge maintains the current snapshot over all retained runs: update by `run_id`, insert new runs, delete Gold verdicts for runs no longer present in any source. `evaluated_at` records the latest evaluation, so it changes on rerun; identical inputs retain the same decision, counts and problem reference.
- The task runs after `create_assertion_result` with [`run_if: ALL_SUCCESS`](https://docs.databricks.com/gcp/en/jobs/run-if). Financial FAIL is stored as data. Fixture assertions require healthy smoke PASS, each bad amount candidate FAIL with exactly one amount mismatch, the bad missing-CDR candidate FAIL with one failed count and five blocked comparisons, and all corrected candidates PASS. Correctly detecting an intentional defect is a successful job execution.

| Count column | Meaning |
| --- | --- |
| `required_assertions` | Number of distinct required session/release/rule keys derived from Silver. |
| `passed_assertions`, `failed_assertions`, `blocked_assertions` | Stored assertion rows with each status, including duplicate or unexpected rows. |
| `missing_assertions` | Required keys with no stored assertion row. |
| `duplicate_assertion_keys` | Assertion keys with more than one stored row. |
| `unexpected_assertions` | Stored rows whose session/release/rule key is outside the required set. |
| `invalid_assertions` | Stored rows with null or unrecognized status. |

These are different diagnostics, not mutually exclusive totals: a duplicate unexpected PASS can count as passed, duplicate and unexpected. Equal passed/required counts alone never grant PASS.

**Current execution boundary:** `release_verdict` remains a mutable diagnostic table; `evaluated_at` alone does not prove freshness. The new `job_execution` and `execution_verdict` contract below protects reads for a specific attempt from stale PASS results after upstream failures. A general manifest inventory of intended sessions, full Silver/source versioning and isolation from external writers remain future work. A session absent from every source is not discoverable without an independently declared session inventory.

The verdict provides the decision and traceable counts. Direct release comparison, event-time first-divergence traces, separate customer overbilling/operator leakage, modeled exposure and its assumptions remain future work; no invented zero monetary totals are stored.

### `execution_verdict` contract

- Destination: `workspace.chargeassert_dev_gold.execution_verdict`; logical key: `(execution_id, run_id)`. The capture task retains the full scenario verdict in `verdict_snapshot` and its complete assertion rows in `assertions_snapshot` as JSON, together with queryable verdicts, counts and reasons. A later evaluation does not rewrite an earlier execution's evidence.
- Capture requires a valid `RUNNING` registration, repair count zero, all thirteen preceding tasks explicitly reporting `success`, seven unique fresh scenario verdicts and complete matching assertion evidence. A skipped/excluded task is insufficient even if the scheduler allows a downstream task to run.
- Insert-only snapshots preserve earlier evidence. Identical retries are idempotent; conflicting reuse is rejected. A new normal execution produces seven snapshots containing ninety-eight assertion records in total. The three deliberate financial FAIL cases are captured alongside the four passing cases. The missing-CDR snapshot includes its five `BLOCKED` assertions without turning a successful test execution into an operational failure.
- Snapshots alone are not a completion signal. Publication can write data before its task fails or is canceled. The finalizer independently requires successful capture, all other required task successes and the full snapshot set before recording `SUCCEEDED`. Readers join the snapshot to that exact successful registration.
- [sql/12_check_execution.sql](../sql/12_check_execution.sql) is the canonical read query. Parameters are `execution_id`, scenario `run_id`, `job_execution_table_name` and `execution_verdict_table_name`. It anchors on the requested identifiers, returns one row even when registration is missing, and returns `BLOCKED` for incomplete, failed, duplicate or missing evidence.
- This boundary assumes the retained fixture inputs are immutable and the configured job runs serially (`max_concurrent_runs: 1`). It does not version every Silver row, isolate manual/external writers, or replace checking the Databricks job's final successful state. Those limits must remain explicit when building the future GitHub gate.

## Current job and deployment

The bundle resource key is `create_tables`; the workspace job name is `chargeassert_dev_create_tables`.

```text
begin_execution → create_run_manifest
  ├─ create_ocpp_transaction_events_raw
  ├─ create_tariffs_raw
  └─ create_ocpi_cdrs_raw

all three Bronze loaders → seed_amount_scenarios
seed_amount_scenarios → generate_mock_billing
generate_mock_billing → create_session_lifecycle + create_tariff_history + create_actual_ledger

create_session_lifecycle + create_tariff_history → create_expected_ledger
create_expected_ledger + create_actual_ledger → create_assertion_result
create_assertion_result → create_release_verdict
all thirteen preceding tasks → capture_execution
all fourteen preceding tasks → finish_execution (ALL_DONE)
```

From the repository root in the environment where you run the authenticated Databricks CLI (`>= 0.295.0`, as required by `databricks.yml`), first obtain the current `dev` code, then update and run the deployed job:

```powershell
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev create_tables
```

Run these in order, continuing only if each command succeeds. `deploy` uploads the SQL, notebooks and Python helpers and updates bundle resources; `run` registers a new execution, creates/populates the fixture tables, executes the mock once, checks the fixtures through Gold and records execution snapshots/completion. The SQL tasks use the configured `Serverless Starter Warehouse`; the mock and tracking notebook tasks use serverless job compute, which must be enabled and available to the job's runtime identity. Use the same workspace authentication as the existing dev deployment.

`TERMINATED SUCCESS` confirms success for the job version that was deployed. The current job should contain **fifteen tasks**, including `begin_execution`, `capture_execution` and `finish_execution`. If any is absent, pull the updated `dev` branch and **deploy before running again**. A Git pull alone does not update the deployed job. It loads and evaluates all seven fixture runs automatically; normal runs need no manual table inserts or parameter overrides. `fail_before_gold` defaults to `false`; use `true` only for the failure demonstration below. The generator and tracking tasks stop when the invocation finishes; another run happens only when the job is started again.

See the [Databricks bundle command reference](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands).

### Check the requested execution

First inspect the execution audit in Databricks SQL and find the row matching the job/run identifiers from the invocation you just started:

```sql
SELECT execution_id, status, reason
FROM workspace.chargeassert_dev_bronze.job_execution
ORDER BY started_at DESC
LIMIT 20;
```

Copy that exact `execution_id`; do not select a different execution because it succeeded. If the requested invocation has no row, construct its `<job_id>:<job_run_id>:0` identifier from the Databricks run details and check it anyway. A missing registration is an incomplete outcome, not permission to use an older PASS.

Run [sql/12_check_execution.sql](../sql/12_check_execution.sql) in SQL Editor with these parameter values:

| Parameter | Value |
| --- | --- |
| `execution_id` | Exact identifier for the requested job attempt |
| `run_id` | `mock-missing-cdr-fixed-v1`, then `mock-missing-cdr-bad-v1` (or another intended fixture) |
| `job_execution_table_name` | `workspace.chargeassert_dev_bronze.job_execution` |
| `execution_verdict_table_name` | `workspace.chargeassert_dev_gold.execution_verdict` |

For a successful full job, check the same exact `execution_id` twice: with `run_id = 'mock-missing-cdr-fixed-v1'`, expect `execution_status = 'SUCCEEDED'`, financial `PASS` and 14 passed / 0 failed; with `run_id = 'mock-missing-cdr-bad-v1'`, expect `SUCCEEDED`, financial `FAIL` and 8 passed / 1 failed. The latter snapshot also contains 5 blocked assertions; the diagnostic query below displays that count. The earlier amount pair still returns 13/1 for bad and 14/0 for fixed. A failed or incomplete execution must return financial `BLOCKED`, with null release verdicts/counts. A missing execution returns `execution_status = 'MISSING'`. Duplicate registration or snapshot rows also block the check.

The SQL below is the same anchored check with the default development table paths. Bind `:execution_id` and `:run_id` in SQL Editor:

```sql
-- Bind the exact execution_id returned for the requested Databricks job attempt.
-- Never substitute the most recent successful execution for a failed/missing one.
-- This query always returns one row, including when registration never happened.
WITH requested AS (
  SELECT CAST(:execution_id AS STRING) AS execution_id, CAST(:run_id AS STRING) AS run_id
), executions AS (
  SELECT execution_id, COUNT(*) AS execution_rows,
    MAX(status) AS execution_status, MAX(reason) AS execution_reason
  FROM workspace.chargeassert_dev_bronze.job_execution
  WHERE execution_id = :execution_id
  GROUP BY execution_id
), snapshots AS (
  SELECT execution_id, run_id, COUNT(*) AS snapshot_rows,
    MAX(baseline_verdict) AS baseline_verdict,
    MAX(candidate_verdict) AS candidate_verdict,
    MAX(verdict) AS verdict, MAX(reason) AS reason,
    MAX(required_assertions) AS required_assertions,
    MAX(passed_assertions) AS passed_assertions,
    MAX(failed_assertions) AS failed_assertions,
    MAX(blocked_assertions) AS blocked_assertions,
    MAX(missing_assertions) AS missing_assertions,
    MAX(duplicate_assertion_keys) AS duplicate_assertion_keys,
    MAX(unexpected_assertions) AS unexpected_assertions,
    MAX(invalid_assertions) AS invalid_assertions
  FROM workspace.chargeassert_dev_gold.execution_verdict
  WHERE execution_id = :execution_id AND run_id = :run_id
  GROUP BY execution_id, run_id
), checked AS (
  SELECT r.execution_id, r.run_id,
    COALESCE(e.execution_rows, 0) AS execution_rows,
    COALESCE(s.snapshot_rows, 0) AS snapshot_rows,
    e.execution_status, e.execution_reason,
    s.baseline_verdict, s.candidate_verdict, s.verdict, s.reason,
    s.passed_assertions, s.failed_assertions,
    CASE WHEN e.execution_rows = 1 AND e.execution_status = 'SUCCEEDED'
      AND s.snapshot_rows = 1
      AND s.baseline_verdict IN ('PASS', 'FAIL')
      AND s.candidate_verdict IN ('PASS', 'FAIL')
      AND s.verdict IN ('PASS', 'FAIL')
      AND (s.verdict <> 'PASS' OR (
        s.baseline_verdict = 'PASS' AND s.candidate_verdict = 'PASS'
        AND s.required_assertions > 0 AND s.passed_assertions = s.required_assertions
        AND s.failed_assertions = 0 AND s.blocked_assertions = 0
        AND s.missing_assertions = 0 AND s.duplicate_assertion_keys = 0
        AND s.unexpected_assertions = 0 AND s.invalid_assertions = 0
      ))
    THEN 1 ELSE 0 END AS usable
  FROM requested AS r
  LEFT JOIN executions AS e ON e.execution_id = r.execution_id
  LEFT JOIN snapshots AS s ON s.execution_id = r.execution_id AND s.run_id = r.run_id
)
SELECT execution_id, run_id,
  CASE WHEN execution_rows = 0 THEN 'MISSING'
    WHEN execution_rows <> 1 THEN 'INVALID' ELSE execution_status END AS execution_status,
  CASE WHEN usable = 1 THEN baseline_verdict END AS baseline_verdict,
  CASE WHEN usable = 1 THEN candidate_verdict END AS candidate_verdict,
  CASE WHEN usable = 1 THEN verdict ELSE 'BLOCKED' END AS verdict,
  CASE WHEN usable = 1 THEN passed_assertions END AS passed_assertions,
  CASE WHEN usable = 1 THEN failed_assertions END AS failed_assertions,
  CASE WHEN execution_rows <> 1 THEN 'No unique registration for the requested execution.'
    WHEN execution_status IS NULL OR execution_status <> 'SUCCEEDED'
      THEN concat('Execution is incomplete or failed: ', COALESCE(execution_reason, 'No completion recorded.'))
    WHEN snapshot_rows <> 1 THEN 'No unique verdict snapshot for this execution and scenario.'
    WHEN usable = 0 THEN 'Invalid financial verdict snapshot.'
    ELSE reason END AS reason
FROM checked;
```

### Prove that an old PASS cannot replace a failed execution

1. Start a normal full job (A), retain its exact execution ID and check the fixed mock: execution `SUCCEEDED`, financial `PASS`.
2. Start a separate full job (B) with the deliberate pre-Gold failure:

   ```powershell
   databricks bundle run -t dev create_tables --params fail_before_gold=true
   ```

   The assertion task fails before modifying Gold. Expect a CLI/job error, a `FAILED` audit record for B when the finalizer runs, and no snapshots for B. Check **B's identifier**: the result must be `BLOCKED` even though A's historical snapshot and the old mutable Gold PASS remain. If finalization is interrupted, `RUNNING`/`MISSING` must still block.
3. Start a new full job (C) with the default parameter:

   ```powershell
   databricks bundle run -t dev create_tables
   ```

   Expect a new successful execution with seven snapshots and all fixture outcomes described here. A's snapshots remain unchanged; B remains a failed/incomplete attempt. Use a new full job, not Repair or a selected subset of tasks.

The failure parameter is a controlled execution test; it does not corrupt raw evidence or change the expected financial fixture outcomes. [Databricks `--params`](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#pass-job-parameters) passes job parameters for that invocation only. This A/B/C workspace demonstration remains pending until run on the deployed fifteen-task job.

### Inspect the current diagnostic tables

The queries below explain the retained fixture data. They read mutable current tables and do **not** replace the exact-execution check above. After a successful full job, inspect the expected ledger:

```sql
SELECT
  run_id,
  session_id,
  tariff_id,
  currency,
  expected_energy_kwh,
  price_per_kwh,
  expected_amount_unrounded,
  expected_amount,
  tariff_valid_from,
  tariff_payload_hash
FROM workspace.chargeassert_dev_silver.expected_ledger
WHERE run_id = 'smoke-run-v1';
```

Expect one row for `txn-smoke-v1` / `tariff-smoke-v1`, with 12.500000 kWh, EUR 0.450000/kWh, EUR 5.625000000000 unrounded and EUR 5.63 rounded. Rerun the job to verify the ledger remains one row. The source tariff remains queryable in `workspace.chargeassert_dev_silver.tariff_history`; its fixture period is `[2026-01-01T00:00:00Z, 2026-12-31T23:59:59Z)`.

Inspect the original smoke raw CDR evidence:

```sql
SELECT
  run_id,
  release_role,
  cdr_id,
  session_id,
  currency,
  total_cost,
  get_json_object(payload, '$.total_energy') AS reported_energy_kwh,
  get_json_object(payload, '$.total_time') AS reported_duration_hours,
  payload_hash
FROM workspace.chargeassert_dev_bronze.ocpi_cdrs_raw
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role;
```

Expect **two rows**, one per release, each with `cdr-smoke-v1`, `txn-smoke-v1`, EUR 5.630000, 12.5 kWh and 1.0 hour. Both hashes should match because the two healthy responses have identical bodies.

Inspect the normalized actual ledger:

```sql
SELECT
  run_id,
  release_role,
  cdr_id,
  session_id,
  actual_energy_kwh,
  actual_duration_hours,
  actual_amount,
  currency,
  tariff_id,
  source_payload_hashes
FROM workspace.chargeassert_dev_silver.actual_ledger
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role;
```

Expect **two actual rows**, one per release, each with 12.500000 kWh, 1.000000 hour and EUR 5.630000. Rerun the job: the expected ledger should still have one row, raw CDRs two rows and the actual ledger two rows for this run.

Inspect the Gold checks:

```sql
SELECT
  release_role, session_id, assertion_id,
  expected_value, actual_value, difference, status, message
FROM workspace.chargeassert_dev_gold.assertion_result
WHERE run_id = 'smoke-run-v1'
ORDER BY release_role, session_id, assertion_id;
```

Expect **14 rows, all PASS**. `amount_match` should show 5.630000 expected, 5.630000 actual and a 0.000000 difference for each release. `duration_match` should show 1.000000 hours. The two textual matches and oracle-availability check have null numeric differences.

Verify the count remains stable after a second run:

```sql
SELECT release_role, status, COUNT(*) AS assertion_count
FROM workspace.chargeassert_dev_gold.assertion_result
WHERE run_id = 'smoke-run-v1'
GROUP BY release_role, status
ORDER BY release_role, status;
```

Expect `baseline / PASS / 7` and `candidate / PASS / 7`. The user confirmed these 14 passing checks on 2026-09-18. To inspect provenance, select `evidence` from the same table for an assertion. Rerun stability still needs checking; local SQLite tests are not proof of a successful Databricks deployment.

Inspect the smoke verdict:

```sql
SELECT
  run_id, baseline_verdict, candidate_verdict, verdict,
  required_assertions, passed_assertions, failed_assertions,
  blocked_assertions, missing_assertions, duplicate_assertion_keys,
  unexpected_assertions, invalid_assertions, reason, first_problem
FROM workspace.chargeassert_dev_gold.release_verdict
WHERE run_id = 'smoke-run-v1';
```

Expect **one row**: baseline PASS, candidate PASS, overall PASS, `required_assertions = 14`, `passed_assertions = 14`, all other counts zero, and `first_problem` null. Rerun the full job and verify the run still has exactly one verdict row and the same outcome. The evaluation timestamp may advance.

To see separate counts per release:

```sql
SELECT
  get_json_object(evidence, '$.baseline') AS baseline_summary,
  get_json_object(evidence, '$.candidate') AS candidate_summary
FROM workspace.chargeassert_dev_gold.release_verdict
WHERE run_id = 'smoke-run-v1';
```

Each summary should show seven required and seven passed assertions, no problems and a PASS verdict. The user confirmed the healthy smoke verdict on 2026-09-19.

Inspect the original canned amount scenarios:

```sql
SELECT run_id, baseline_verdict, candidate_verdict, verdict,
       required_assertions, passed_assertions, failed_assertions,
       blocked_assertions, missing_assertions
FROM workspace.chargeassert_dev_gold.release_verdict
WHERE run_id IN ('amount-bad-v1', 'amount-fixed-v1')
ORDER BY run_id;
```

| Run | Baseline | Candidate | Verdict | Required | Passed | Failed | Blocked | Missing |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `amount-bad-v1` | PASS | FAIL | FAIL | 14 | 13 | 1 | 0 | 0 |
| `amount-fixed-v1` | PASS | PASS | PASS | 14 | 14 | 0 | 0 | 0 |

Inspect the exact amount comparison:

```sql
SELECT run_id, expected_value, actual_value, difference, status, evidence
FROM workspace.chargeassert_dev_gold.assertion_result
WHERE run_id IN ('amount-bad-v1', 'amount-fixed-v1')
  AND release_role = 'candidate'
  AND assertion_id = 'amount_match'
ORDER BY run_id;
```

The bad candidate must show **5.630000 expected / 6.500000 actual / +0.870000 / FAIL**; the corrected candidate must show **5.630000 / 5.630000 / 0.000000 / PASS**. Both baselines remain PASS. The user confirmed the canned pair's verdicts and 13/1 versus 14/0 assertion counts in Databricks on 2026-09-20.

Inspect the **Python-generated runs** in the current diagnostic verdict table:

```sql
SELECT run_id, baseline_verdict, candidate_verdict, verdict,
       required_assertions, passed_assertions, failed_assertions,
       blocked_assertions, missing_assertions
FROM workspace.chargeassert_dev_gold.release_verdict
WHERE run_id IN ('mock-amount-bad-v1', 'mock-amount-fixed-v1')
ORDER BY run_id;
```

| Run | Baseline | Candidate | Verdict | Required | Passed | Failed | Blocked | Missing |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `mock-amount-bad-v1` | PASS | FAIL | FAIL | 14 | 13 | 1 | 0 | 0 |
| `mock-amount-fixed-v1` | PASS | PASS | PASS | 14 | 14 | 0 | 0 | 0 |

```sql
SELECT run_id, release_role, expected_value, actual_value, difference, status
FROM workspace.chargeassert_dev_gold.assertion_result
WHERE run_id IN ('mock-amount-bad-v1', 'mock-amount-fixed-v1')
  AND assertion_id = 'amount_match'
ORDER BY run_id, release_role;
```

Expect four rows. Only `mock-amount-bad-v1 / candidate` should show **5.630000 expected / 6.500000 actual / +0.870000 / FAIL**. The other three rows should show **5.630000 / 5.630000 / 0.000000 / PASS**. Query Bronze `ocpi_cdrs_raw` with these run IDs to inspect the generated `cdr-mock-v1` bodies and payload hashes. The user confirmed the generated pair's verdicts and 13/1 versus 14/0 counts on 2026-09-21; the new execution-tracking deployment and failure demonstration remain pending.

Inspect the **missing-CDR pair** after the exact-execution check succeeds operationally:

```sql
SELECT run_id, baseline_verdict, candidate_verdict, verdict,
       required_assertions, passed_assertions, failed_assertions,
       blocked_assertions, missing_assertions
FROM workspace.chargeassert_dev_gold.release_verdict
WHERE run_id IN ('mock-missing-cdr-bad-v1', 'mock-missing-cdr-fixed-v1')
ORDER BY run_id;
```

| Run | Baseline | Candidate | Verdict | Required | Passed | Failed | Blocked | Missing |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `mock-missing-cdr-bad-v1` | PASS | FAIL | FAIL | 14 | 8 | 1 | 5 | 0 |
| `mock-missing-cdr-fixed-v1` | PASS | PASS | PASS | 14 | 14 | 0 | 0 | 0 |

`missing_assertions = 0` means all required checks were produced. The missing **CDR** is the recorded failed count check; it is not a missing assertion.

```sql
SELECT run_id, assertion_id, expected_value, actual_value,
       difference, status, message
FROM workspace.chargeassert_dev_gold.assertion_result
WHERE run_id IN ('mock-missing-cdr-bad-v1', 'mock-missing-cdr-fixed-v1')
  AND release_role = 'candidate'
ORDER BY run_id, assertion_id;
```

Expect fourteen rows. In the bad run, `oracle_available` is `PASS`; `final_cdr_count` is **1.000000 expected / 0.000000 actual / -1.000000 / FAIL**; the five value comparisons are `BLOCKED`. In particular, `amount_match` retains **5.630000 expected**, with a null actual value and a null difference. All seven candidate checks in the fixed run pass.

To show the absent raw record explicitly, anchor the count on the intended runs and roles:

```sql
WITH runs AS (
  SELECT 'mock-missing-cdr-bad-v1' AS run_id
  UNION ALL SELECT 'mock-missing-cdr-fixed-v1'
), roles AS (
  SELECT 'baseline' AS release_role
  UNION ALL SELECT 'candidate'
)
SELECT r.run_id, roles.release_role, COUNT(c.payload_hash) AS raw_cdr_count
FROM runs AS r
CROSS JOIN roles
LEFT JOIN workspace.chargeassert_dev_bronze.ocpi_cdrs_raw AS c
  ON c.run_id = r.run_id AND c.release_role = roles.release_role
GROUP BY r.run_id, roles.release_role
ORDER BY r.run_id, roles.release_role;
```

Expect four rows: bad baseline **1**, bad candidate **0**, fixed baseline **1**, fixed candidate **1**. A plain grouped query over CDR rows alone would omit the missing candidate rather than display zero. These are diagnostic current-table queries; retain the exact-execution snapshot for historical proof. Databricks confirmation of this pair remains pending.

For all seven fixture runs combined, expect 7 manifests, 21 raw events, 7 raw tariffs, 13 raw CDRs, 7 lifecycle rows, 7 tariff-history rows, 7 expected-ledger rows, 13 actual-ledger rows, 98 assertion rows and 7 verdict rows. Filter counts to the seven fixture run IDs if other data exists. Rerun the full job with identical code and inputs: these counts and raw payload hashes should remain stable, including zero candidate CDRs for the bad missing-CDR run. All three intentionally faulty runs must retain their FAIL verdicts. `evaluated_at` may advance. A drift error requires investigating the changed input/code and using new versioned run IDs for an intentional change.

Execution history intentionally grows: each successful new full job adds one `job_execution` row and seven `execution_verdict` snapshots containing ninety-eight assertion records as JSON. Identical retries within one attempt do not add duplicate snapshots. A pre-Gold failure adds its audit record but no verdict snapshots; interrupted registration/finalization remains an incomplete outcome. Earlier completed five-run receipts and snapshots remain unchanged and queryable by exact ID. The ten fixture-table counts above are unchanged by execution tracking.

## Remaining MVP work

| Workstream | Still missing |
| --- | --- |
| Reproducible inputs | A general versioned scenario registry, a seeded event generator, real baseline/candidate Git SHAs and complete input snapshot hashes. Current fixtures have fixed inputs/times. Canned runs use synthetic SHA labels; generated mock runs fingerprint module bytes and behavior. New paired manifests hash the tariff contents; the original smoke manifest still hashes an identifier. |
| Public-data provenance and ingestion | Versioned ACN-Data behavior and French IRVE station snapshots, synthetic composition/provenance documentation, GCS input storage and Auto Loader ingestion. The combined data must not be presented as real French transactions or actual operator tariffs. |
| Replay and fault injection | Amount-surcharge and missing-CDR mock pairs are implemented on identical inputs; the missing-CDR pair still needs workspace verification. Add duplicate charge and the other required scenarios, then external release/HTTP replay. Event/retry traces that identify the first divergence remain missing. These mocks do not execute actual software release builds. |
| Session validation | Select the appropriate meter measurand, normalize units/multipliers, handle meter resets, validate timestamps and sequence numbers, deduplicate transport retries, define late/missing/conflicting-event behavior, and preserve station identity when forming session keys. |
| Tariff selection | Load multiple tariff periods in Bronze and replace the explicit seven-fixture mapping with general scenario-defined session/tariff associations. Start-time selection is implemented; tariff-boundary pricing remains unsupported. Extend pricing only when a scenario requires it. |
| Independent oracle | Generalize beyond the seven mapped fixtures to validated scenario energy/duration. Decimal amounts, HALF_UP session-total rounding, per-session input guards and explicit comparison precision are implemented; broader scenarios remain. Keep the SQL calculation independent from the Python mock implementation. |
| Actual records | Extend computed mock ingestion to external replay and beyond the supported final-CDR subset. Normalization, equivalent-delivery deduplication and conflict guards are implemented; structured Gold conflict reporting, broader Session/CDR contracts and session-ID mapping remain. The original canned fixtures remain regression evidence. |
| Assertions | The healthy 14-check smoke result is verified in Databricks; independent oracle, CDR count, energy, duration, currency, tariff ID and amount checks have local faulty-input coverage. Add direct baseline/candidate comparison, tariff-version evidence, retry/idempotency and late-event invariants, and structured upstream-failure reporting. |
| Verdict and evidence | Verify execution tracking and the A/B/C failure demonstration in Databricks. Execution registration, immutable verdict/assertion snapshots and upstream-failure blocking are implemented for the fixed seven-run inventory. General manifest session inventory, full source/Silver version binding, external-write isolation, first-divergence traces and separate customer overbilling/operator leakage remain. Mock fingerprints identify module/behavior, not executed Git release builds. |
| Modeled exposure | Calculate defect-rate delta × assumed monthly sessions × assumed impact per affected session, expose assumptions and separate overbilling from leakage. Label projections as modeled exposure, never actual losses or proven savings. |
| GitHub automation | Add GitHub Actions, authenticated Databricks execution, exact execution/scenario retrieval, a PASS/FAIL check with evidence links, and required-check configuration for the release gate. Require final Databricks job success as well as a successful execution registration and passing snapshot; never fall back to a previous success. |
| Verification and demo | Both canned and executable amount mock FAIL → PASS outcomes are verified in Databricks. The missing-CDR pair and execution boundary need deployment and verification, including the A/B/C failure proof and interrupted/partial-run checks. Repeat-run stability, Databricks integration tests, broader scenarios and deterministic full-run checks remain. The PDF's 50,000 sessions and EUR 24,380 report are illustrative, not measured results. |
| Runtime access | Define explicit grants when introducing a separate CI/runtime identity; current development relies on schema ownership. |

### Six flagship scenarios

| Scenario | Required demonstration |
| --- | --- |
| Missing CDR | Implemented in the mock; workspace verification pending. A completed billable session produces no candidate final CDR, fails the count check and blocks dependent comparisons. Potential unbilled value is visible through the independent expectation; aggregate revenue-leakage reporting remains missing. |
| Duplicate charge | A retry produces two different CDR IDs for the same session; preserve and report both. |
| Wrong energy | Meter order, reset or units distort billed kWh; detect the mismatch against validated evidence. |
| Wrong tariff | The wrong effective price/version is used; expose the tariff and amount mismatch. |
| Retry failure | An HTTP failure breaks idempotency; show the first retry where behavior diverges. |
| Late event | Out-of-order delivery changes the final outcome; validate the final state against the same logical session. |

## Next implementation step

1. Pull `dev`, validate, deploy and run the fifteen-task job above. Verify the exact execution's seven snapshots, the missing-CDR bad run's 8 PASS / 1 FAIL / 5 BLOCKED checks, the corrected run's 14 PASS checks and the unchanged earlier amount outcomes. Perform the A/B/C failure demonstration: the failed B execution must return `BLOCKED` even while A's PASS remains stored. Confirm another full job succeeds without rewriting earlier snapshots.
2. Add the duplicate-charge scenario: the faulty candidate returns two different final CDR IDs for the same session, while the corrected candidate returns one. Preserve both records, require `final_cdr_count` to fail on two records and block the dependent comparisons instead of summing the charges or selecting one arbitrarily.
3. Generalize the scenario/session inventory, strengthen session validation and source version binding, then expand to wrong energy, wrong tariff, retry failure and late events, seeded event generation, HTTP replay, public-data ingestion, modeled exposure, GitHub gate and documented portfolio demonstration.

Real card/payment processing, bank/PSP/ERP/settlement integration, full OCPP/OCPI certification, production monitoring/recovery, every tariff/tax/currency, machine learning and confidential operator data remain outside the MVP.

## Permissions status

- The deployment user was previously documented as owning all three schemas. Confirm current grants in the workspace when deploying with another identity.
- No explicit grants for another user, group, service principal or CI identity are defined in the bundle yet.
- A separate runtime identity needs `USE CATALOG` on `workspace`, `USE SCHEMA` and `CREATE TABLE` on the relevant schemas, plus `SELECT` and `MODIFY` for tables it reads or writes without owning them. It also needs access to the job, SQL warehouse and serverless notebook/job compute.
- Table readers need `USE CATALOG`, `USE SCHEMA` and `SELECT`.

```sql
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_bronze;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_silver;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_gold;
```

Use explicit least-privilege grants when introducing CI or collaborators. All current fixtures are synthetic.
