# 01 — MVP tables

ChargeAssert will use managed Delta tables inside the three Unity Catalog schemas that already exist:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

The schemas are deployed, but they are empty. The tables below are the minimal plan for the first complete test flow.

## Bronze — preserve the evidence

Bronze stores the original input and candidate output without changing their meaning.

| Table | One row represents | Important fields |
| --- | --- | --- |
| `run_manifest` | One deterministic test run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `ocpp_events_raw` | One received OCPP event | `run_id`, `event_id`, `event_type`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpi_cdrs_raw` | One CDR returned by the candidate release | `run_id`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `ingest_time` |
| `tariffs_raw` | One input tariff version | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `payload`, `payload_hash` |

## Silver — produce trusted business records

Silver validates, deduplicates and normalizes the raw evidence.

| Table | One row represents | Important fields |
| --- | --- | --- |
| `session_lifecycle` | One logical charging session in one run | `run_id`, `session_id`, `started_at`, `ended_at`, `meter_start_wh`, `meter_end_wh`, `status` |
| `tariff_history` | One effective tariff period | `tariff_id`, `valid_from`, `valid_to`, `currency`, `price_components` |
| `expected_ledger` | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `expected_amount`, `currency`, `tariff_id` |
| `actual_ledger` | One CDR actually returned by the candidate | `run_id`, `session_id`, `cdr_id`, `actual_energy_kwh`, `actual_amount`, `currency`, `tariff_id` |

`actual_ledger` deliberately keeps more than one row per session when duplicate CDRs exist. ChargeAssert must detect duplicates, not hide them.

## Gold — make the release decision

Gold contains the explainable test results used by GitHub and a future dashboard.

| Table | One row represents | Important fields |
| --- | --- | --- |
| `assertion_result` | One rule checked for one session | `run_id`, `session_id`, `assertion_id`, `expected_value`, `actual_value`, `difference`, `status`, `severity` |
| `release_verdict` | The final decision for one candidate run | `run_id`, `candidate_sha`, `passed_assertions`, `failed_assertions`, `financial_exposure`, `verdict`, `created_at` |

The GitHub check will eventually read `release_verdict.verdict`: `PASS` allows the release and `FAIL` blocks it.

## Permissions status

- The deployment user currently owns all three schemas, so that same user can create and manage the first managed Delta tables.
- No explicit grants for another user, group, service principal or CI job are defined in the bundle yet.
- A separate runtime identity will need `USE CATALOG` on `workspace`, plus `USE SCHEMA` and `CREATE TABLE` on each ChargeAssert schema. It will also need `SELECT` and `MODIFY` when it reads or writes tables that it does not own.
- Table readers need `USE CATALOG`, `USE SCHEMA` and `SELECT`.

Check the current schema grants in Databricks SQL:

```sql
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_bronze;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_silver;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_gold;
```

For the current single-user development stage, ownership is enough. Explicit least-privilege grants should be added when the first automated job or collaborator is introduced.

## Next implementation step

Create these tables with version-controlled SQL, run that SQL from a Databricks job, and verify the tables in Catalog Explorer. No production data or credentials will be used.
