# 01 — MVP tables and remaining work

The product scope is defined in [OVERVIEW.pdf](OVERVIEW.pdf). The MVP replays the same deterministic scenario against a baseline and a candidate, compares both with an independent financial calculation, and produces an explainable GitHub PASS/FAIL verdict. A deliberately faulty candidate must fail; its corrected version must pass the exact same manifest.

## Current implementation status

ChargeAssert uses managed Delta tables inside these Unity Catalog schemas:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

Eight tables now have version-controlled SQL wired into the `create_tables` job. **Implemented means code exists, not that its latest version has been deployed or successfully run.** The user confirmed the session/tariff preview in Databricks on 2026-09-06: one row with 12.500000 kWh, EUR 0.450000/kWh and EUR 5.625000000000 before rounding. A successful job output shown on 2026-09-12 still listed only the five older tasks. The `expected_ledger`, `ocpi_cdrs_raw` and `actual_ledger` tasks need deployment and execution in that workspace.

The current fixture is one run (`smoke-run-v1`), one session (`txn-smoke-v1`), three OCPP-shaped events, one EUR energy tariff, and two fixed CDR responses (one baseline and one candidate). These responses are seeded literals; a replay adapter does not exist yet. This is a foundation smoke test, not the complete release gate or the six-scenario suite.

## Bronze — preserve the evidence

Bronze preserves original inputs and, once implemented, baseline and candidate outputs.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `run_manifest` | Implemented | One deterministic test run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `ocpp_transaction_events_raw` | Implemented | One received OCPP 2.0.1-shaped `TransactionEvent` | `run_id`, `event_id`, `charging_station_id`, `transaction_id`, `event_type`, `sequence_number`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpi_cdrs_raw` | Implemented with fixed smoke responses; deployment pending | One distinct CDR payload captured for one release in one run | `run_id`, `release_role`, `country_code`, `party_id`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `payload_hash`, `ingest_time` |
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
| `expected_ledger` | Implemented for smoke session; deployment pending | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `price_per_kwh`, `expected_amount_unrounded`, `expected_amount`, `currency`, `tariff_id`, `tariff_valid_from`, `tariff_payload_hash` |
| `actual_ledger` | Implemented for the final-CDR subset; deployment pending | One normalized CDR returned by either release | `run_id`, `release_role`, `country_code`, `party_id`, `session_id`, `cdr_id`, `cdr_type`, `started_at`, `ended_at`, `actual_energy_kwh`, `actual_duration_hours`, `actual_amount`, `currency`, `tariff_id`, `source_payload_hashes` |

Fields listed for missing tables are proposed contracts, not existing columns. `actual_ledger` must preserve different CDR IDs for the same session and release. Repeated delivery of the same transport event is deduplicated; two different financial records for one billable session are preserved and reported as a business defect.

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

They exercise the production normalization SQL with SQLite adapters for parsed fields/functions, plus job wiring. They cover release/run isolation, exact and equivalent deliveries, distinct CDR preservation, conflicts, invalid inputs and fractional-cent retention. They do **not** execute Databricks `from_json`, Delta DDL or `MERGE`; the job smoke assertions still require a workspace run.

## Gold — make the release decision

Gold will contain the explainable results read by GitHub. A dashboard is future work, not a prerequisite for the MVP.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `assertion_result` | Missing | One rule checked for one session and release | `run_id`, `release_role`, `session_id`, `assertion_id`, `expected_value`, `actual_value`, `difference`, `status`, `severity`, evidence reference |
| `release_verdict` | Missing | The final decision for one candidate run | `run_id`, `baseline_sha`, `candidate_sha`, `passed_assertions`, `failed_assertions`, overbilling exposure, leakage exposure, exposure assumptions, first-divergence reference, `verdict`, `created_at` |

The GitHub check will read `release_verdict.verdict`: `PASS` allows the release and `FAIL` blocks it. Baseline/candidate differences must be reported alongside independent assertions, so a baseline defect cannot become an accepted expected result.

## Current job and deployment

The bundle resource key is `create_tables`; the workspace job name is `chargeassert_dev_create_tables`.

```text
create_run_manifest
  ├─ create_ocpp_transaction_events_raw → create_session_lifecycle
  ├─ create_tariffs_raw → create_tariff_history
  └─ create_ocpi_cdrs_raw → create_actual_ledger

create_session_lifecycle + create_tariff_history → create_expected_ledger
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

`TERMINATED SUCCESS` confirms success for the job version that was deployed. If the output lists only the five older tasks and omits `create_expected_ledger`, `create_ocpi_cdrs_raw` and `create_actual_ledger`, pull the updated `dev` branch and **deploy before running again**. A Git pull alone does not update the deployed job. The current job should contain eight tasks. Raw CDRs belong in the **Bronze** schema; the two ledgers belong in **Silver**.

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

## Remaining MVP work

| Workstream | Still missing |
| --- | --- |
| Reproducible inputs | Versioned scenario manifests, a seeded event generator, fixed event clock, real baseline/candidate Git SHAs, input snapshot hashes and a reproduction command. Current SHA values are placeholders; `run_manifest.tariff_hash` hashes an identifier rather than the tariff contents. |
| Public-data provenance and ingestion | Versioned ACN-Data behavior and French IRVE station snapshots, synthetic composition/provenance documentation, GCS input storage and Auto Loader ingestion. The combined data must not be presented as real French transactions or actual operator tariffs. |
| Replay and fault injection | Controlled baseline/candidate mocks, identical replay inputs/IDs/timestamps, output isolation and event/retry traces that can identify the first divergence. |
| Session validation | Select the appropriate meter measurand, normalize units/multipliers, handle meter resets, validate timestamps and sequence numbers, deduplicate transport retries, define late/missing/conflicting-event behavior, and preserve station identity when forming session keys. |
| Tariff selection | Load multiple tariff periods in Bronze and replace the explicit smoke mapping with scenario-defined session/tariff associations. Start-time selection is implemented for the fixture; tariff-boundary pricing remains unsupported. Extend pricing only when a scenario requires it. |
| Independent oracle | Deploy and verify the smoke `expected_ledger`, then generalize it to scenario runs and validated energy/duration. Decimal amounts and HALF_UP session-total rounding are implemented; comparison tolerances, broader scenarios and invalid-input regression coverage remain. Keep the calculation independent from the mock release implementation. |
| Actual records | Deploy and verify raw CDRs and `actual_ledger`, replace canned responses with mock/replay ingestion, and extend beyond the supported final-CDR subset. Normalization, equivalent-delivery deduplication and conflict guards are implemented; structured Gold conflict reporting, broader Session/CDR contracts and session-ID mapping remain. |
| Assertions | Implement `assertion_result`: exactly one final CDR for a completed billable session; correct energy/duration, tariff and amount; retry/idempotency and late-event invariants. Test the baseline and candidate against the oracle as well as comparing releases. |
| Verdict and evidence | Implement `release_verdict`, first-divergence evidence and separate customer overbilling/operator leakage. Include run ID, seed, SHAs, snapshot hashes and a reproduction command. |
| Modeled exposure | Calculate defect-rate delta × assumed monthly sessions × assumed impact per affected session, expose assumptions and separate overbilling from leakage. Label projections as modeled exposure, never actual losses or proven savings. |
| GitHub automation | Add GitHub Actions, authenticated Databricks execution, verdict retrieval, a PASS/FAIL check with evidence links, and required-check configuration for the release gate. |
| Verification and demo | Local actual-ledger regression checks exist. Add Databricks integration tests, scenario/financial-rule coverage, deterministic full-run checks, a complete faulty-candidate FAIL → corrected-candidate PASS demonstration, and setup/replay/report documentation. The PDF's 50,000 sessions and EUR 24,380 report are illustrative, not measured results. |
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

1. Pull `dev`, validate, deploy and run the eight-task foundation job above. Confirm one expected-ledger row, two raw CDR rows and two actual-ledger rows, then rerun to verify stable counts.
2. Implement Gold `assertion_result`: compare each release's actual records against the independent expected ledger for missing/duplicate CDRs and energy, duration, currency, tariff and amount differences. Start with the healthy fixture, then a separate faulty scenario that demonstrates a failed assertion without changing expected values.
3. Complete one financial test flow with baseline/candidate mocks, CDR evidence, ledgers, assertions and a verdict. Show a faulty candidate failing and its correction passing identical inputs.
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
