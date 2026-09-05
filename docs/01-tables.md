# 01 — MVP tables and remaining work

The product scope is defined in [OVERVIEW.pdf](OVERVIEW.pdf). The MVP replays the same deterministic scenario against a baseline and a candidate, compares both with an independent financial calculation, and produces an explainable GitHub PASS/FAIL verdict. A deliberately faulty candidate must fail; its corrected version must pass the exact same manifest.

## Current implementation status

ChargeAssert uses managed Delta tables inside these Unity Catalog schemas:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

The schemas were previously documented as deployed. Five tables now have version-controlled SQL wired into the `create_tables` job. **Implemented means code exists, not that its latest version has been deployed or successfully run.** The new `tariff_history` task still needs workspace validation, deployment and execution.

The current fixture is one run (`smoke-run-v1`), one session (`txn-smoke-v1`), three OCPP-shaped events, and one EUR energy tariff. It is a foundation smoke test, not the complete release gate or the six-scenario suite.

## Bronze — preserve the evidence

Bronze preserves original inputs and, once implemented, baseline and candidate outputs.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `run_manifest` | Implemented | One deterministic test run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `ocpp_transaction_events_raw` | Implemented | One received OCPP 2.0.1-shaped `TransactionEvent` | `run_id`, `event_id`, `charging_station_id`, `transaction_id`, `event_type`, `sequence_number`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpi_cdrs_raw` | Missing | One CDR returned by either release | `run_id`, `release_role`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `payload_hash`, `ingest_time` |
| `tariffs_raw` | Implemented | One input tariff version in a run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `payload`, `payload_hash` |

`release_role` is a planned discriminator (`baseline` or `candidate`), so outputs from the two systems cannot overwrite one another. The current manifest's two SHA columns alone do not provide output isolation.

## Silver — produce trusted business records

Silver validates, deduplicates and normalizes the raw evidence.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `session_lifecycle` | Implemented, happy path only | One logical charging session in one run | `run_id`, `session_id`, `started_at`, `ended_at`, `meter_start_wh`, `meter_end_wh`, `status` |
| `tariff_history` | Implemented; deployment pending | One effective tariff period in one run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `price_components`, `source_payload_hash` |
| `expected_ledger` | Missing | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `expected_amount`, `currency`, `tariff_id`, `tariff_valid_from` |
| `actual_ledger` | Missing | One CDR actually returned by either release | `run_id`, `release_role`, `session_id`, `cdr_id`, `actual_energy_kwh`, `actual_amount`, `currency`, `tariff_id` |

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
  └─ create_tariffs_raw → create_tariff_history
```

From the repository root, using an authenticated Databricks CLI (`>= 0.295.0`, as required by `databricks.yml`):

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev create_tables
```

Run these in order, continuing only if each command succeeds. `deploy` uploads the SQL and updates bundle resources; `run` executes the SQL that creates/populates the tables and checks the fixtures. The configured SQL warehouse lookup is `Serverless Starter Warehouse`. Use the same workspace authentication as the existing dev deployment.

See the [Databricks bundle command reference](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands).

After the job succeeds, inspect the new table in Databricks SQL:

```sql
SELECT
  run_id,
  tariff_id,
  valid_from,
  valid_to,
  currency,
  price_components,
  source_payload_hash
FROM workspace.chargeassert_dev_silver.tariff_history
WHERE run_id = 'smoke-run-v1';
```

Expect one row for `tariff-smoke-v1`, EUR, with one ENERGY component at `0.450000` and step size `1`. The fixture period is `[2026-01-01T00:00:00Z, 2026-12-31T23:59:59Z)`. Rerun the job to verify the fixture remains one row.

## Remaining MVP work

| Workstream | Still missing |
| --- | --- |
| Reproducible inputs | Versioned scenario manifests, a seeded event generator, fixed event clock, real baseline/candidate Git SHAs, input snapshot hashes and a reproduction command. Current SHA values are placeholders; `run_manifest.tariff_hash` hashes an identifier rather than the tariff contents. |
| Public-data provenance and ingestion | Versioned ACN-Data behavior and French IRVE station snapshots, synthetic composition/provenance documentation, GCS input storage and Auto Loader ingestion. The combined data must not be presented as real French transactions or actual operator tariffs. |
| Replay and fault injection | Controlled baseline/candidate mocks, identical replay inputs/IDs/timestamps, output isolation and event/retry traces that can identify the first divergence. |
| Session validation | Select the appropriate meter measurand, normalize units/multipliers, handle meter resets, validate timestamps and sequence numbers, deduplicate transport retries, define late/missing/conflicting-event behavior, and preserve station identity when forming session keys. |
| Tariff selection | Load multiple tariff periods in Bronze, explicitly associate a session with its tariff/version, and define effective-time selection and tariff-boundary behavior. Extend supported pricing only when a scenario requires it. |
| Independent oracle | Implement `expected_ledger`: validated energy/duration, effective tariff, decimal amount calculation, explicit rounding and comparison tolerances. Keep the calculation independent from the mock release implementation. |
| Actual records | Implement `ocpi_cdrs_raw` and `actual_ledger`, normalize baseline/candidate CDRs and retain business duplicates. Define the OCPI-shaped Session/CDR contracts and session-ID mapping. |
| Assertions | Implement `assertion_result`: exactly one final CDR for a completed billable session; correct energy/duration, tariff and amount; retry/idempotency and late-event invariants. Test the baseline and candidate against the oracle as well as comparing releases. |
| Verdict and evidence | Implement `release_verdict`, first-divergence evidence and separate customer overbilling/operator leakage. Include run ID, seed, SHAs, snapshot hashes and a reproduction command. |
| Modeled exposure | Calculate defect-rate delta × assumed monthly sessions × assumed impact per affected session, expose assumptions and separate overbilling from leakage. Label projections as modeled exposure, never actual losses or proven savings. |
| GitHub automation | Add GitHub Actions, authenticated Databricks execution, verdict retrieval, a PASS/FAIL check with evidence links, and required-check configuration for the release gate. |
| Verification and demo | Add scenario/financial-rule regression tests, deterministic rerun checks, a complete faulty-candidate FAIL → corrected-candidate PASS demonstration, and setup/replay/report documentation. The PDF's 50,000 sessions and EUR 24,380 report are illustrative, not measured results. |
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

1. Validate, deploy and run the five-task foundation job above; check the normalized tariff and rerun behavior.
2. Implement `expected_ledger` using `session_lifecycle` and `tariff_history`. The fixture delivers `(112500 - 100000) / 1000 = 12.5 kWh`; at EUR 0.45/kWh its unrounded expected charge is EUR 5.625. Define rounding before asserting a final amount.
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
