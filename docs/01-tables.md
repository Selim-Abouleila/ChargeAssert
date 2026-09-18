# 01 — MVP tables and remaining work

The product scope is defined in [OVERVIEW.pdf](OVERVIEW.pdf). The MVP replays the same deterministic scenario against a baseline and a candidate, compares both with an independent financial calculation, and produces an explainable GitHub PASS/FAIL verdict. A deliberately faulty candidate must fail; its corrected version must pass the exact same manifest.

## Current implementation status

ChargeAssert uses managed Delta tables inside these Unity Catalog schemas:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

Ten tables now have version-controlled SQL wired into the `create_tables` job. **Implemented means code exists, not that its latest version has been deployed or successfully run.** On 2026-09-18 the user confirmed all 14 Gold smoke assertions PASS in Databricks: seven baseline and seven candidate checks, including 12.500000 kWh, 1.000000 hour and EUR 5.630000. The latest addition is Gold `release_verdict`; its deployment and the full ten-task job remain to be verified in the workspace.

The current fixture is one run (`smoke-run-v1`), one session (`txn-smoke-v1`), three OCPP-shaped events, one EUR energy tariff, and two fixed CDR responses (one baseline and one candidate). These responses are seeded literals; a replay adapter does not exist yet. This is a foundation smoke test, not the complete release gate or the six-scenario suite.

## Bronze — preserve the evidence

Bronze preserves original inputs and the fixed baseline and candidate outputs.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `run_manifest` | Implemented | One deterministic test run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `ocpp_transaction_events_raw` | Implemented | One received OCPP 2.0.1-shaped `TransactionEvent` | `run_id`, `event_id`, `charging_station_id`, `transaction_id`, `event_type`, `sequence_number`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpi_cdrs_raw` | Implemented with fixed smoke responses | One distinct CDR payload captured for one release in one run | `run_id`, `release_role`, `country_code`, `party_id`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `payload_hash`, `ingest_time` |
| `tariffs_raw` | Implemented | One input tariff version in a run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `payload`, `payload_hash` |

`release_role` (`baseline` or `candidate`) now isolates the raw CDR outputs. The two smoke responses intentionally use the same CDR ID and payload to verify that both releases retain their own evidence. The current manifest's two SHA columns alone would not provide output isolation.

### `ocpi_cdrs_raw` contract

- Destination: `workspace.chargeassert_dev_bronze.ocpi_cdrs_raw`. Its job task depends only on `run_manifest`, keeping reported release outputs independent of the expected-charge calculation.
- The first loader seeds two canned responses for `smoke-run-v1`: `baseline` and `candidate`, both reporting `cdr-smoke-v1` for `txn-smoke-v1`, 12.5 kWh, one hour and EUR 5.63. Neither response is read from `expected_ledger`.
- The financial subset uses [OCPI 2.2.1 CDR field names](https://github.com/ocpi/ocpi/blob/release-2.2.1-bugfixes/mod_cdrs.asciidoc). `total_energy` is in kWh, `total_time` is in hours and `total_cost` is a Price object. The fixture omits full token/location data and does not claim protocol certification.
- Preserve the exact JSON body and SHA-256 hash. The insert-only merge key is `(run_id, release_role, payload_hash)`; an exact retry leaves the first capture unchanged. This is a logical key, not a database-enforced uniqueness constraint.
- Different CDR IDs for the same session remain separate because their payloads differ. Changed payloads with the same CDR ID are also retained, so Silver can report a conflicting financial record instead of silently replacing it. Changes in JSON formatting also remain separate raw payloads; semantic deduplication belongs in Silver.
- `country_code`, `party_id` and `cdr_id` preserve the reported record identity. Silver `actual_ledger` uses those owner fields as well as the CDR ID and release/run identity.
- Extracted business fields are nullable so unparseable or incomplete reported values can remain in the raw payload for later validation. `total_cost` is a convenience `DECIMAL(18,6)` projection of `payload.total_cost.excl_vat`, with no cent rounding; the payload retains the original precision. VAT-inclusive comparison is outside this fixture's contract.
- `cdr_type` is ChargeAssert metadata derived from the OCPI `credit` flag: `FINAL` for false/absent, `CREDIT` for true, and null for an invalid flag. Credit handling is not implemented in the billing comparison yet.
- The smoke assertion checks exactly two intact, healthy responses and their extracted values. This is fixture verification, not a rule rejecting faulty candidate outputs from Bronze. Fault scenarios will need separate runs and assertions.
- Still missing: an actual mock/replay adapter, ingestion of external response files, and delivery-attempt/event traces. Silver validation/deduplication is implemented for the subset below. Rerunning this fixture loader checks stored records; it does not exercise a real HTTP retry.

## Silver — produce trusted business records

Silver validates, deduplicates and normalizes the raw evidence.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `session_lifecycle` | Implemented, happy path only | One logical charging session in one run | `run_id`, `session_id`, `started_at`, `ended_at`, `meter_start_wh`, `meter_end_wh`, `status` |
| `tariff_history` | Implemented; smoke join verified in Databricks | One effective tariff period in one run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `price_components`, `source_payload_hash` |
| `expected_ledger` | Implemented for smoke session; Gold smoke checks verified | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `price_per_kwh`, `expected_amount_unrounded`, `expected_amount`, `currency`, `tariff_id`, `tariff_valid_from`, `tariff_payload_hash` |
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
- The first implementation prices only `smoke-run-v1` / `txn-smoke-v1`, explicitly mapped to `tariff-smoke-v1`. Other sessions are outside this smoke task; a scenario-defined session-to-tariff mapping is still needed.
- Require one completed session with nonnegative, nondecreasing meter readings and valid start/end timestamps, plus exactly one matching tariff effective at the session start. Missing or ambiguous matches fail the task.
- The supported tariff is one EUR ENERGY component with `step_size = 1`. The full session must fit within the selected period; sessions crossing a tariff boundary fail until boundary pricing is implemented.
- Energy is `(meter_end_wh - meter_start_wh) / 1000`, stored as `DECIMAL(18,6)`. Price is `DECIMAL(18,6)` and their unrounded product is retained as `DECIMAL(37,12)`.
- Round the session total once using **HALF_UP to two decimal places**; store `expected_amount` as `DECIMAL(18,2)`. This is the chosen synthetic MVP contract, not a claim about every operator's billing rules. Databricks [`round(amount, 2)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/round) uses HALF_UP.
- Retain the selected tariff ID, period start, payload hash, currency and rate for traceability. Reruns update the same logical ledger row.
- The smoke assertion requires 12.5 kWh at EUR 0.45/kWh, EUR 5.625 unrounded and **EUR 5.63 rounded**. Meter normalization/reset handling still depends on future session validation work.

### `actual_ledger` contract

- Source: Bronze `ocpi_cdrs_raw`; destination: `workspace.chargeassert_dev_silver.actual_ledger`. The job depends only on raw CDRs. It does not join to the expected ledger, session evidence or input tariffs to replace reported values.
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

They exercise the production normalization, assertion and verdict SELECT queries with SQLite adapters for parsed fields, timestamps, arrays/JSON and HALF_UP rounding, plus job wiring. They cover release/run isolation, exact and equivalent deliveries, distinct CDR preservation, conflicts, invalid inputs, fractional-cent retention, missing/duplicate CDR assertions, financial mismatches, oracle gaps, evidence and a faulty-candidate FAIL to corrected-candidate PASS comparison against unchanged expectations. Verdict tests also cover missing entire releases/sessions, duplicate checks replacing missing checks, unsupported rules/roles/statuses, empty runs and missing/duplicate manifests. They do **not** execute Databricks `from_json`, Spark type analysis/decimal arithmetic, Delta DDL or `MERGE`; workspace execution is still required for each new task.

## Gold — make the release decision

Gold stores explainable checks and a first run-level verdict. The GitHub integration and a dashboard are future work.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `assertion_result` | Implemented; 14 smoke PASS rows verified in Databricks | One rule checked for one session and release | `run_id`, `release_role`, `session_id`, `assertion_id`, `expected_value`, `actual_value`, `difference`, `status`, `severity`, `message`, `evidence` |
| `release_verdict` | Implemented for current assertions; deployment pending | The current decision for one run, including both release outcomes | `run_id`, manifest provenance, assertion/coverage counts, `baseline_verdict`, `candidate_verdict`, `verdict`, `reason`, `first_problem`, `evidence`, `evaluated_at` |

The future GitHub check must require a successful current full job and a matching `release_verdict.verdict = 'PASS'` for the requested run. A failed job, missing verdict or `FAIL` verdict must block the release. Direct baseline/candidate differences still need reporting alongside the independent assertions; a shared billing defect already fails both releases against the oracle.

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

This first implementation consumes the supported final-CDR Silver contract. It does not yet compare releases directly, verify the full reported tariff version/content, compare reported start/end timestamps, report Silver parsing/conflict failures as Gold rows, or assert HTTP retry/late-event traces. The expected-ledger producer still prices only the smoke session; general scenarios need their own independent expectations. The GitHub gate remains unimplemented.

### `release_verdict` contract

- Sources: Bronze `run_manifest`, Gold `assertion_result`, and Silver `expected_ledger`, `session_lifecycle` and `actual_ledger`. Destination: `workspace.chargeassert_dev_gold.release_verdict`; logical key: `run_id`.
- Produce one row for every run found in any of those tables. A manifest with no sessions/checks gets FAIL; data with no manifest also gets FAIL. Exactly one manifest is required. Copy `scenario_id`, `seed`, `baseline_sha`, `candidate_sha` and `tariff_hash` only when the manifest is unique; missing/duplicate manifests leave this provenance null. The existing smoke SHAs remain placeholders, not validated source commits.
- Derive required assertion keys independently from Silver using the same session scope as `assertion_result`: expected-ledger and completed-lifecycle sessions require both release roles; supported FINAL actual-only sessions require their reporting role. Cross those keys with the explicit seven-rule registry. An entire missing session/release cannot disappear by reducing the observed assertion count.
- Each release needs a nonempty required set, exactly one PASS row per required key, and no failed, blocked, duplicate, unexpected or invalid-status assertions. Both `baseline_verdict` and `candidate_verdict` must be PASS for the overall `verdict` to pass. Unknown-role assertions also fail the overall verdict even when the two known releases independently pass.
- Verdicts are `PASS` or `FAIL`. Blocked assertions are retained in the counts but produce a FAIL verdict. A failed baseline blocks the overall run even if the candidate passes; matching baseline/candidate mistakes do not establish correctness.
- `reason` explains the highest-priority problem. `first_problem` is a JSON reference containing problem type and available run/release/session/assertion keys. Order is deterministic: manifest error, empty coverage, missing check, duplicate check, unexpected check, invalid status, FAIL, then BLOCKED; ties sort by release/session/assertion. This is an investigation entry point, **not the first chronological divergence**. For existing checks, retrieve `assertion_result` by these keys to inspect values, messages and original evidence.
- `evidence` contains rule version, manifest-row count and separate `baseline`/`candidate` summaries with coverage counts and verdicts. Run-level totals also include assertions with unsupported release roles.
- The merge maintains the current snapshot over all retained runs: update by `run_id`, insert new runs, delete Gold verdicts for runs no longer present in any source. `evaluated_at` records the latest evaluation, so it changes on rerun; identical inputs retain the same decision, counts and problem reference.
- The task runs after `create_assertion_result` with [`run_if: ALL_SUCCESS`](https://docs.databricks.com/gcp/en/jobs/run-if). A financial FAIL in an ordinary scenario is stored as data; the SQL task itself need not fail. Only the fixed healthy smoke run has an explicit PASS smoke assertion.

| Count column | Meaning |
| --- | --- |
| `required_assertions` | Number of distinct required session/release/rule keys derived from Silver. |
| `passed_assertions`, `failed_assertions`, `blocked_assertions` | Stored assertion rows with each status, including duplicate or unexpected rows. |
| `missing_assertions` | Required keys with no stored assertion row. |
| `duplicate_assertion_keys` | Assertion keys with more than one stored row. |
| `unexpected_assertions` | Stored rows whose session/release/rule key is outside the required set. |
| `invalid_assertions` | Stored rows with null or unrecognized status. |

These are different diagnostics, not mutually exclusive totals: a duplicate unexpected PASS can count as passed, duplicate and unexpected. Equal passed/required counts alone never grant PASS.

**Current execution boundary:** the tables have no execution/snapshot ID or manifest inventory of intended sessions. The verdict cannot detect a session absent from every source, stale PASS assertions after an upstream failure, or a new parsing/conflict error before Gold runs. If the current full job fails or is skipped, do not consume a prior verdict as current success. `evaluated_at` alone does not prove freshness. Structured execution-error reporting, source-version binding and complete manifest coverage remain required before an automated production-style gate.

This microstep adds the decision and traceable counts only. Direct release comparison, event-time first-divergence traces, separate customer overbilling/operator leakage, modeled exposure and its assumptions remain future work; no invented zero monetary totals are stored.

## Current job and deployment

The bundle resource key is `create_tables`; the workspace job name is `chargeassert_dev_create_tables`.

```text
create_run_manifest
  ├─ create_ocpp_transaction_events_raw → create_session_lifecycle
  ├─ create_tariffs_raw → create_tariff_history
  └─ create_ocpi_cdrs_raw → create_actual_ledger

create_session_lifecycle + create_tariff_history → create_expected_ledger
create_expected_ledger + create_actual_ledger → create_assertion_result
create_assertion_result → create_release_verdict
```

From the repository root in the environment where you run the authenticated Databricks CLI (`>= 0.295.0`, as required by `databricks.yml`), first obtain the current `dev` code, then update and run the deployed job:

```powershell
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev create_tables
```

Run these in order, continuing only if each command succeeds. `deploy` uploads the SQL and updates bundle resources; `run` executes the SQL that creates/populates the tables and checks the fixtures. The configured SQL warehouse lookup is `Serverless Starter Warehouse`. Use the same workspace authentication as the existing dev deployment.

`TERMINATED SUCCESS` confirms success for the job version that was deployed. If the output omits newer tasks such as `create_release_verdict`, pull the updated `dev` branch and **deploy before running again**. A Git pull alone does not update the deployed job. The current job should contain **ten tasks**, ending with `create_release_verdict`. Raw CDRs belong in **Bronze**, the two ledgers in **Silver**, and assertions/verdicts in **Gold**.

See the [Databricks bundle command reference](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands).

After the job succeeds, inspect the expected ledger in Databricks SQL:

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

Inspect the newly added raw CDR evidence:

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

Inspect the newly added verdict:

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

Each summary should show seven required and seven passed assertions, no problems and a PASS verdict. This new verdict task still needs workspace validation.

## Remaining MVP work

| Workstream | Still missing |
| --- | --- |
| Reproducible inputs | Versioned scenario manifests, a seeded event generator, fixed event clock, real baseline/candidate Git SHAs, input snapshot hashes and a reproduction command. Current SHA values are placeholders; `run_manifest.tariff_hash` hashes an identifier rather than the tariff contents. |
| Public-data provenance and ingestion | Versioned ACN-Data behavior and French IRVE station snapshots, synthetic composition/provenance documentation, GCS input storage and Auto Loader ingestion. The combined data must not be presented as real French transactions or actual operator tariffs. |
| Replay and fault injection | Controlled baseline/candidate mocks, identical replay inputs/IDs/timestamps, output isolation and event/retry traces that can identify the first divergence. |
| Session validation | Select the appropriate meter measurand, normalize units/multipliers, handle meter resets, validate timestamps and sequence numbers, deduplicate transport retries, define late/missing/conflicting-event behavior, and preserve station identity when forming session keys. |
| Tariff selection | Load multiple tariff periods in Bronze and replace the explicit smoke mapping with scenario-defined session/tariff associations. Start-time selection is implemented for the fixture; tariff-boundary pricing remains unsupported. Extend pricing only when a scenario requires it. |
| Independent oracle | Generalize the verified smoke `expected_ledger` to scenario runs and validated energy/duration. Decimal amounts, HALF_UP session-total rounding and explicit first-subset comparison precision are implemented; broader scenarios and invalid-input regression coverage remain. Keep the calculation independent from the mock release implementation. |
| Actual records | Replace canned responses with mock/replay ingestion, and extend beyond the supported final-CDR subset. Normalization, equivalent-delivery deduplication and conflict guards are implemented; structured Gold conflict reporting, broader Session/CDR contracts and session-ID mapping remain. |
| Assertions | The healthy 14-check smoke result is verified in Databricks; independent oracle, CDR count, energy, duration, currency, tariff ID and amount checks have local faulty-input coverage. Add direct baseline/candidate comparison, tariff-version evidence, retry/idempotency and late-event invariants, and structured upstream-failure reporting. |
| Verdict and evidence | Deploy and verify the initial `release_verdict`. Add execution/snapshot binding, manifest session inventory, upstream-error outcomes, first-divergence traces, separate customer overbilling/operator leakage and a reproduction command. Stored manifest SHAs/hashes remain smoke placeholders. |
| Modeled exposure | Calculate defect-rate delta × assumed monthly sessions × assumed impact per affected session, expose assumptions and separate overbilling from leakage. Label projections as modeled exposure, never actual losses or proven savings. |
| GitHub automation | Add GitHub Actions, authenticated Databricks execution, verdict retrieval, a PASS/FAIL check with evidence links, and required-check configuration for the release gate. |
| Verification and demo | Local actual-ledger, assertion and verdict regression checks exist, including faulty/corrected verdicts against unchanged expectations. Add Databricks integration tests, broader scenario coverage, deterministic full-run checks, a complete replay-to-verdict faulty-candidate FAIL → corrected-candidate PASS demonstration, and setup/replay/report documentation. The PDF's 50,000 sessions and EUR 24,380 report are illustrative, not measured results. |
| Runtime access | Define explicit grants when introducing a separate CI/runtime identity; current development relies on schema ownership. |

### Six flagship scenarios still to implement

| Scenario | Required demonstration |
| --- | --- |
| Missing CDR | A completed billable session produces no final CDR; report revenue leakage. |
| Duplicate charge | A retry produces two different CDR IDs for the same session; preserve and report both. |
| Wrong energy | Meter order, reset or units distort billed kWh; detect the mismatch against validated evidence. |
| Wrong tariff | The wrong effective price/version is used; expose the tariff and amount mismatch. |
| Retry failure | An HTTP failure breaks idempotency; show the first retry where behavior diverges. |
| Late event | Out-of-order delivery changes the final outcome; validate the final state against the same logical session. |

## Next implementation step

1. Pull `dev`, validate, deploy and run the ten-task foundation job above. Confirm the 14 PASS assertion rows plus one PASS release verdict with 14 passed checks and no coverage problems, then rerun to verify stable counts.
2. Add one separate deterministic faulty-candidate scenario with independent expected inputs and mock outputs. Verify a complete flow through Bronze, Silver and Gold yields FAIL, then yields PASS after correcting the candidate while preserving the logical scenario inputs. Do not overwrite the healthy smoke fixture or rely solely on manually editing a verdict row.
3. Bind outputs to the current execution and complete manifest session coverage, capture upstream-error results, and add replay evidence/reproduction instructions before relying on an automated release gate.
4. Expand to the six scenarios, public-data ingestion, modeled exposure, GitHub gate and documented portfolio demonstration.

Real card/payment processing, bank/PSP/ERP/settlement integration, full OCPP/OCPI certification, production monitoring/recovery, every tariff/tax/currency, machine learning and confidential operator data remain outside the MVP.

## Permissions status

- The deployment user was previously documented as owning all three schemas. Confirm current grants in the workspace when deploying with another identity.
- No explicit grants for another user, group, service principal or CI identity are defined in the bundle yet.
- A separate runtime identity needs `USE CATALOG` on `workspace`, `USE SCHEMA` and `CREATE TABLE` on the relevant schemas, plus `SELECT` and `MODIFY` for tables it reads or writes without owning them. It also needs access to the job and SQL warehouse.
- Table readers need `USE CATALOG`, `USE SCHEMA` and `SELECT`.

```sql
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_bronze;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_silver;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_gold;
```

Use explicit least-privilege grants when introducing CI or collaborators. All current fixtures are synthetic.
