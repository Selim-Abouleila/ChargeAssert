# 02 — Table rules, setup and verification

For a short overview of each table and its business purpose, start with the [table guide](01-tables.md).

ChargeAssert helps EV charging teams find billing errors before a software release affects customers or leaves sessions unbilled. [OVERVIEW.pdf](OVERVIEW.pdf) defines the full MVP scope.

The intended release test gives the same repeatable charging scenario to a baseline version and a candidate version. It checks both against an independent calculation of what the charge should be. A deliberately faulty candidate must fail; its corrected version must pass with the same recorded inputs. The current code demonstrates this with fixed responses and Python billing mocks. It stores the PASS/FAIL decisions in Databricks; publishing a GitHub release check is still planned.

[Current status](#current-implementation-status) · [Generated billing](#evaluate-generated-batches-through-gold) · [Generate sessions](#generate-new-charging-sessions) · [File ingestion](#incremental-ocpp-file-ingestion) · [Local checks](#local-checks-and-their-limits) · [Deploy and run](#billing-regression-job-and-deployment) · [Remaining work](#remaining-mvp-work)

## Current implementation status

ChargeAssert uses managed Delta tables inside these Unity Catalog schemas:

| Layer | Schema |
| --- | --- |
| Bronze | `workspace.chargeassert_dev_bronze` |
| Silver | `workspace.chargeassert_dev_silver` |
| Gold | `workspace.chargeassert_dev_gold` |

The repository defines **26 tables and six manual jobs**. The original thirteen tables cover the fifteen-task `create_tables` billing test job and shared `ocpp_events_landing` table. The new `evaluate_generated_batch` job has thirteen separate `generated_*` tables for a selected input batch. The remaining jobs are `generate_sessions`, `publish_ingestion_demo`, `ingest_ocpp_files` and `combined_ingestion`. The combined job starts generation, then calls the existing ingestion job; it does not run billing by itself.

**Implemented means the code exists. It does not mean the latest version has been deployed or tested successfully in Databricks.**

The following results have been confirmed in Databricks:

- On 2026-09-18, all 14 Gold checks passed for the smoke test. On 2026-09-19, its release verdict was confirmed as PASS with 14/14 checks.
- On 2026-09-20, the bad and corrected fixed-response tests were confirmed.
- On 2026-09-21, the Python-generated amount tests were confirmed. `mock-amount-bad-v1` has a passing baseline, a failing candidate and overall FAIL, with 13 passing / 1 failing checks. `mock-amount-fixed-v1` has PASS for both releases and overall, with 14 passing / 0 failing checks.
- Execution `192226331898541:436138745571440:0`, started on 2026-09-23, was confirmed as `SUCCEEDED`, with seven saved result snapshots for seven distinct scenarios. Earlier failed attempts remain `FAILED` with no snapshots.

The successful execution confirms that the original result capture works after the Spark filter fix. The exact financial values in those snapshots and the deliberate failure test before Gold still need checking. On 2026-10-04, a [generated Gold result](03-generated-sessions.md#confirmed-result) was also confirmed: two sessions, 28 checks and all three verdicts PASS. Its job completion record, deliberate-error test, repeat runs and recovery still need checking.

The job keeps three runs with fixed responses (`smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`) and four runs whose responses are calculated by the mock (`mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1`, `mock-missing-cdr-fixed-v1`). Each uses one session (`txn-smoke-v1`), the same three OCPP-shaped charging events and one EUR energy tariff. A CDR, or charge detail record, is the billing record returned for a session.

The Python mock calculates the reported charge from the raw meter events and tariff. Silver SQL calculates the expected charge separately; this independent calculation is called the **oracle**. The bad amount candidates report EUR 6.50; healthy responses report EUR 5.63. The bad missing-CDR candidate returns no billing record. Every other run has one baseline and one candidate CDR. These are small synthetic tests; the complete release gate and six-scenario suite are still planned.

The billing mock runs once when its job is started and then exits. The separate session generator does the same: it creates one file containing a batch of events on request. There is no schedule, continuous loop, HTTP server or background service.

A scenario's `run_id` identifies its saved test inputs and outputs. An `execution_id` identifies one Databricks job attempt that checks them. Every full job run gets its own audit record and result snapshots. Always query the exact execution you want to check. An old PASS in `release_verdict` cannot show that a newer failed or unfinished execution passed.

## Bronze — preserve the evidence

Bronze preserves original inputs and the captured baseline and candidate outputs. The contracts in this section describe the original fixed-scenario tables. The [generated-batch section](#evaluate-generated-batches-through-gold) describes their separate `generated_*` counterparts and the extra intended-session inventory.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `run_manifest` | Implemented | One repeatable test scenario run | `run_id`, `scenario_id`, `seed`, `baseline_sha`, `candidate_sha`, `tariff_hash`, `created_at` |
| `job_execution` | Implemented; successful seven-snapshot execution confirmed | One Databricks job attempt and its completion state | `execution_id`, Databricks job/run identifiers, repair count, `status`, `expected_run_ids_json`, start/finish timestamps, task states, `reason` |
| `ocpp_transaction_events_raw` | Implemented | One received OCPP 2.0.1-shaped `TransactionEvent` | `run_id`, `event_id`, `charging_station_id`, `transaction_id`, `event_type`, `sequence_number`, `event_time`, `ingest_time`, `payload`, `payload_hash` |
| `ocpp_events_landing` | Implemented; ingestion demo verification pending | One source text line received through Auto Loader | `raw_record`, `record_hash`, `source_file_path`, `source_file_name`, `source_file_modification_time`, `ingested_at` |
| `ocpi_cdrs_raw` | Implemented with saved responses and computed mock responses | One distinct CDR payload captured for one release in one run | `run_id`, `release_role`, `country_code`, `party_id`, `cdr_id`, `session_id`, `cdr_type`, `currency`, `total_cost`, `payload`, `payload_hash`, `ingest_time` |
| `tariffs_raw` | Implemented | One input tariff version in a run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `payload`, `payload_hash` |

`release_role` (`baseline` or `candidate`) keeps the two releases' raw CDRs separate. The two smoke responses deliberately use the same CDR ID and payload to check that each release keeps its own evidence. The manifest's two SHA columns alone would not separate these outputs.

### `run_manifest` contract

- Destination: `workspace.chargeassert_dev_bronze.run_manifest`; logical key: `run_id`. Each row identifies a test scenario, its seed, baseline and candidate identifiers, tariff hash and creation time.
- `sql/01_create_run_manifest.sql` inserts `smoke-run-v1` with `scenario_id = 'smoke-scenario-v1'`, seed 42 and the fixed timestamp `2026-08-22T00:00:00Z`. Rerunning leaves the existing row unchanged. The task displays that smoke row after loading it.
- The smoke identifiers are `sha1('baseline-smoke')` and `sha1('candidate-smoke')`. These are synthetic labels, not tested Git commits. Later computed mocks use fingerprints of their source code and behavior; they do not run Git release builds either.
- The smoke `tariff_hash` is SHA-256 of the identifier `tariff-smoke-v1`. Later amount and missing-CDR manifests hash the tariff payload itself. The fixed tests record seed 42, but do not yet use it to generate varied events.
- The original smoke loader uses an insert-only merge. It does not compare every saved value against a changed fixture definition. The later paired-test and mock loaders add stricter checks for conflicting evidence. This original fixture manifest is still fixed. Generated batches use a separate complete session list and input/code hashes, described below; a broader scenario registry is still planned.

### `ocpp_transaction_events_raw` contract

- Destination: `workspace.chargeassert_dev_bronze.ocpp_transaction_events_raw`; logical key: `(run_id, event_id)`. It keeps the exact OCPP 2.0.1-shaped `TransactionEvent` JSON and its SHA-256 hash, plus station, session, event type, sequence number and timestamps for querying.
- `sql/02_create_ocpp_transaction_events_raw.sql` first requires exactly one parent `smoke-run-v1` manifest. It then inserts three events for station `cs-smoke-001` and session `txn-smoke-v1`: `Started`, `Updated` and `Ended`, with sequence numbers 0, 1 and 2.
- The events occur on 2026-08-22 at 10:00, 10:30 and 11:00 UTC. Their cumulative meter readings are 100000, 106000 and 112500 Wh. Each fixed `ingest_time` is one second after its `event_time`. The session therefore supplies the inputs for a one-hour, 12.5 kWh test.
- The merge only inserts missing keys. The smoke check requires three rows, three distinct event IDs, one of each expected event-type/sequence pair and three valid payload hashes. The task displays the events in sequence order, including meter readings and hashes.
- These checks do not compare every saved byte with a changed fixture definition. Existing rows are not rewritten. This original loader uses fixed test events. The generated evaluator reads the landing table and writes `generated_ocpp_transaction_events_raw`; it does not add generated events to this original table.

### `tariffs_raw` contract

- Destination: `workspace.chargeassert_dev_bronze.tariffs_raw`; logical key: `(run_id, tariff_id)`. It keeps the original tariff JSON, its SHA-256 hash, currency and validity dates.
- `sql/04_create_tariffs_raw.sql` inserts `tariff-smoke-v1` for `smoke-run-v1`: one `ENERGY` price component at EUR 0.45/kWh with `step_size = 1`. Its fixed validity runs from `2026-01-01T00:00:00Z` to `2026-12-31T23:59:59Z`.
- The merge only inserts missing keys. The smoke check requires exactly one row with the expected tariff ID, EUR currency and a matching payload hash. The task then displays the saved tariff. It does not compare every field with a changed fixture definition.
- The raw key does not include `valid_from`. It cannot yet keep several validity periods for the same tariff ID within one run. More general tariff history needs this input contract to expand, as well as the Silver rules described below.

### `job_execution` contract

- Destination: `workspace.chargeassert_dev_bronze.job_execution`; logical key: `execution_id`. This table records whether a job completed. It sits alongside the original Bronze evidence and does not represent a charging session.
- `execution_id` is `<job_id>:<job_run_id>:<repair_count>`, taken from the Databricks job context. Every full new job run gets a new identifier, even when the seven scenario run IDs and input bytes are unchanged. Invalid or unresolved context is rejected.
- The begin task records `RUNNING` before the pipeline starts. It saves the seven required scenario IDs in `expected_run_ids_json`: `smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`, `mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1` and `mock-missing-cdr-fixed-v1`. This is a fixed list of test scenarios. A general list of every intended session is not implemented yet.
- The finish task records `SUCCEEDED` only when every required earlier task explicitly succeeded and the execution has seven complete, unique snapshots. Otherwise it records `FAILED`, saves the task states and a reason, then raises an error so the job also reports failure. Before accepting a result, check that this finish task and the Databricks job itself both finished successfully. The audit row confirms earlier tasks and saved snapshots; it cannot guarantee that the finish task completed after writing the row.
- A successful job can correctly capture an intentional financial `FAIL` and its dependent `BLOCKED` checks. These test results do not, by themselves, mean that the job failed to run.
- A canceled job or unavailable compute can prevent the finish task from running. Its execution may stay `RUNNING`; if the begin task never registered it, the identifier may be absent. The check query returns `BLOCKED` for these unfinished or missing attempts. It never substitutes a previous PASS.
- Job repairs are rejected in this version. After correcting a failure, start a new full job. A partial repair may reuse successful tasks from an earlier attempt, so it does not prove that the whole pipeline ran again. Only one job run is allowed at a time.
- Earlier completed executions used five scenarios. Their saved snapshots remain unchanged and can still be read by exact execution ID. The seven-scenario list applies to new full jobs. Do not repair an old attempt to add the new scenarios.

The tracking code is in `notebooks/execution_tracking.py`. `notebooks/track_execution.py` calls it in begin, capture and finish modes. Job context and task outcomes come from [Databricks dynamic references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references). The finish task uses [All done dependencies](https://docs.databricks.com/gcp/en/jobs/run-if), so it can run after an earlier task fails. It still cannot write a completion record if it is itself canceled or fails.

### `ocpi_cdrs_raw` contract

- Destination: `workspace.chargeassert_dev_bronze.ocpi_cdrs_raw`. The original smoke loader depends only on `run_manifest`. The amount-test loader reads Bronze inputs and responses, while the Python generator calculates new responses from Bronze events and tariffs. None takes its reported charge from the independent expected-charge calculation.
- The first loader adds two fixed responses for `smoke-run-v1`: `baseline` and `candidate`. Both report `cdr-smoke-v1` for `txn-smoke-v1`, 12.5 kWh, one hour and EUR 5.63. Neither response comes from `expected_ledger`.
- The financial subset uses [OCPI 2.2.1 CDR field names](https://github.com/ocpi/ocpi/blob/release-2.2.1-bugfixes/mod_cdrs.asciidoc). `total_energy` is in kWh, `total_time` is in hours and `total_cost` is a Price object. The fixture omits full token/location data and does not claim protocol certification.
- The table keeps the exact JSON body and its SHA-256 hash. The insert-only merge uses `(run_id, release_role, payload_hash)` as its key; an exact retry leaves the first saved record unchanged. The loading code uses this key, but the database does not enforce its uniqueness.
- Different CDR IDs for the same session remain separate because their payloads differ. A changed payload with the same CDR ID is also kept, so Silver can report a conflict rather than overwrite it. Even changes to JSON formatting remain separate raw payloads. Silver decides which payloads describe the same billing record.
- `country_code`, `party_id` and `cdr_id` preserve the reported record identity. Silver `actual_ledger` uses those owner fields as well as the CDR ID and release/run identity.
- Extracted business fields allow nulls. If a value cannot be read or is missing, the original payload remains available for later checks. `total_cost` is a convenient `DECIMAL(18,6)` copy of `payload.total_cost.excl_vat`, without rounding to cents. The payload keeps the original precision. These tests do not compare VAT-inclusive amounts.
- `cdr_type` is a ChargeAssert field based on the OCPI `credit` flag: `FINAL` when false or absent, `CREDIT` when true, and null when invalid. The billing comparison does not handle credits yet.
- The smoke check requires exactly two intact, healthy responses with the expected extracted values. Separate amount runs keep the faulty response without changing this healthy test data. Bronze checks that the evidence is intact and matches the fixture; Gold checks whether the billing is correct.
- Still missing: replay against external release software over HTTP, loading external response files, and traces of delivery attempts or events. Silver validates and removes duplicate records for the supported subset below, and a small billing mock runs within Python. Rerunning the loaders or generator checks saved records; it does not test a real HTTP retry.

### Paired amount fixtures

`sql/11_seed_amount_scenarios.sql` runs after the original Bronze loaders and before the three Silver transformations. It adds data to the existing tables; it creates no new table.

| Run | Baseline response | Candidate response | Independent expectation | Expected verdict |
| --- | --- | --- | --- | --- |
| `smoke-run-v1` | EUR 5.63 | EUR 5.63 | EUR 5.63 | PASS |
| `amount-bad-v1` | EUR 5.63 | EUR 6.50 | EUR 5.63 | FAIL |
| `amount-fixed-v1` | EUR 5.63 | EUR 5.63 | EUR 5.63 | PASS |

- Both amount manifests use `scenario_id = 'amount-mismatch-v1'`, seed 42, the same fixed timestamps and the same baseline identifier. Both runs use identical OCPP event bytes, tariff bytes, logical event/session/CDR IDs and input tariff hash. Separate run IDs keep their evidence apart.
- Candidate identifiers are repeatable synthetic labels: `sha1('candidate-amount-bad-v1')` and `sha1('candidate-amount-fixed-v1')`. They do not identify tested Git commits. This pair uses fixed outputs to show the expected checks; it does not run a real code fix or replay a mock.
- The corrected response has a separate run ID, so the original EUR 6.50 evidence and FAIL verdict can still be queried. The two manifests share the same scenario inputs. Neither replaces the other.
- New manifests hash the actual tariff payload with SHA-256. The original smoke manifest stays unchanged and still hashes the tariff identifier. A complete record of all input snapshots and their origins is future work.
- The seed task reads only Bronze evidence. It copies the healthy responses, replaces exactly one fixed amount in the bad candidate body, then recalculates the extracted `total_cost` and `payload_hash`. It never copies a value from `expected_ledger` or writes a Gold decision directly.
- Four insert-only merges use the existing Bronze keys. Checks before and after loading reject missing, duplicate or conflicting test rows. Rerunning keeps one manifest, three events, one tariff and two raw CDRs per run. A changed fixture needs a new version/run ID; old evidence must not be overwritten.
- The expected ledger separately recalculates the charge from meter differences and the selected tariff: 12.5 × 0.45 = 5.625, rounded HALF_UP to 5.63. The bad run must fail only candidate `amount_match`, with **actual minus expected = +0.870000**. Its baseline passes; the corrected run has 14 passing checks.
- The final verdict task checks these expected outcomes. **A successful demo job includes intentional financial FAIL verdicts.** The job itself fails if a bad case unexpectedly passes, a fixed case fails, or any required test data is missing. The fixed-response pair's 13/1 and 14/0 outcomes were confirmed in Databricks on 2026-09-20.

### On-demand mock billing

`notebooks/mock_billing.py` calculates the amount-test responses using only the Python standard library. `notebooks/mock_missing_cdr.py` adds the missing-CDR behavior, and `generate_all_mock_runs` runs both pairs. `notebooks/generate_mock_billing.py` calls this combined generator in the existing `generate_mock_billing` notebook task after `seed_amount_scenarios`. It saves the evidence in the existing Bronze tables, so the missing-CDR addition needs no new table or task.

| Run | Baseline behavior | Candidate behavior | Expected candidate amount | Expected verdict |
| --- | --- | --- | --- | --- |
| `mock-amount-bad-v1` | Healthy calculation | Deliberate EUR 0.87 surcharge | EUR 6.50 | FAIL |
| `mock-amount-fixed-v1` | Healthy calculation | Healthy calculation | EUR 5.63 | PASS |

- For the amount pair, the mock reads the same raw smoke event and tariff JSON for both releases and both runs. It calculates energy from the meter difference, duration from the event timestamps and the energy rate from the tariff. It uses `Decimal` and rounds the session amount HALF_UP to cents. It does not read Silver, Gold or a fixed CDR to get its result.
- The faulty behavior adds EUR 0.87 to the calculated amount; the healthy behavior returns it unchanged. Both create the supported OCPI-shaped financial fields with `cdr_id = 'cdr-mock-v1'`. Full token/location data and protocol certification are not included.
- The original event/tariff bytes, logical input IDs and fixed clock stay the same. New run IDs keep generated evidence separate from the earlier fixed responses, whose raw payloads and verdicts remain queryable. These fixed scenarios do not yet use the seed to generate different events.
- The amount manifests hash the source tariff payload. Their `baseline_sha` and `candidate_sha` are fingerprints of the original Python billing module's bytes and selected behavior. They identify the mock code and behavior, not tested Git release commits. The bad and corrected amount runs share the healthy baseline fingerprint but have different candidate fingerprints.
- Insert-only merges keep the first evidence, and checks reject reuse of a run ID with different code, inputs or outputs. For the amount pair, identical reruns keep one manifest, three events, one tariff and two raw CDRs per run. Changed mock code or inputs need new versioned run IDs and updated test expectations. Do not rewrite saved evidence to make a rerun pass.
- Silver separately prices all seven mapped sessions, including the missing-CDR session where the candidate returns nothing. The generated bad amount candidate must fail only `amount_match` at +EUR 0.87. Its baseline passes, and the generated corrected run has 14 passing checks. Gold decides the financial result; the generator never writes a PASS/FAIL decision.
- The mock runs once per manually started job. It needs Databricks serverless notebook/job compute as well as the existing SQL warehouse. It adds no Python packages, schedule or continuously running process. See the [Databricks serverless job bundle example](https://docs.databricks.com/aws/en/dev-tools/bundles/examples#job-that-uses-serverless-compute).
- Both generated verdicts and their 13/1 versus 14/0 check counts were confirmed in Databricks on 2026-09-21. A later full execution was confirmed `SUCCEEDED` with all seven snapshots. The deliberate failure test before Gold still needs verification.

### Missing-CDR mock scenario

This scenario tests a completed charging session with no final billing record. `notebooks/mock_missing_cdr.py` reuses the original mock's input checks and healthy billing calculation, then deliberately drops the faulty candidate's CDR. Both releases still have the raw events and tariff, so the independent oracle expects 12.5 kWh, one hour and EUR 5.63.

| Run | Baseline CDRs | Candidate CDRs | Baseline / candidate / overall verdict | Passed | Failed | Blocked |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| `mock-missing-cdr-bad-v1` | 1 | 0 | PASS / FAIL / FAIL | 8 | 1 | 5 |
| `mock-missing-cdr-fixed-v1` | 1 | 1 | PASS / PASS / PASS | 14 | 0 | 0 |

- Both runs use `scenario_id = 'mock-missing-cdr-v1'`, seed 42, session `txn-smoke-v1` and tariff `tariff-smoke-v1`, with identical input bytes and a fixed clock. Separate run IDs keep both the faulty and corrected evidence.
- The bad run has no candidate CDR in Bronze and no candidate row in Silver `actual_ledger`. The generator does not insert a null placeholder or a made-up zero charge. It also checks that no unexpected candidate record already exists for this run ID.
- The bad candidate passes `oracle_available`. It fails `final_cdr_count` with **expected 1, actual 0, difference -1**. `energy_match`, `duration_match`, `currency_match`, `tariff_match` and `amount_match` are `BLOCKED` because there is no reported record to compare. The expected amount is still EUR 5.63; the reported amount and amount difference are null.
- All seven baseline checks pass in both runs. In the corrected run, the candidate returns one healthy CDR and all fourteen checks pass. The existing Gold rules decide these results, and additional test checks require the expected demo outcomes.
- `mock_billing.py` stays byte-for-byte unchanged because existing amount manifests fingerprint its source. Missing-CDR fingerprints include both that helper's source and the new module's source, normalized to LF line endings, plus the chosen behavior (`healthy-v1` or `drop-final-cdr-v1`). They identify the mock code, not an executed Git release build. Changed code or inputs need new versioned run IDs; saved evidence must not be overwritten.
- The missing record represents a **potentially unbilled EUR 5.63 synthetic session**. It does not prove real revenue loss. Totals for revenue at risk and modeled financial exposure are not calculated yet. The mock runs within Python; it does not record real HTTP delivery or retry attempts.
- These cases were part of the confirmed successful execution with seven snapshots. Their exact saved financial values still need direct inspection. Rerunning identical code and inputs must keep one record in the bad run and two in the corrected run.

### Incremental OCPP file ingestion

This path loads newly arrived files into Delta and remembers which files it has processed between manually started jobs. Ingestion stops at the landing table. Run `evaluate_generated_batch` afterward to check a selected batch through its separate Bronze, Silver and Gold tables; the original fixed-test loaders stay independent.

```text
data/ingestion_demo/batch_001.jsonl or batch_002.jsonl
  → publish_ingestion_demo
  → managed Volume /incoming/ ← generate_sessions (new synthetic sessions)
  → ingest_ocpp_files (Auto Loader, AvailableNow)
  → Bronze ocpp_events_landing
```

The bundle defines the managed Unity Catalog Volume `workspace.chargeassert_dev_bronze.ocpp_ingestion`. Its storage layout is:

```text
/Volumes/workspace/chargeassert_dev_bronze/ocpp_ingestion/
  incoming/
    batch_001.jsonl
    batch_002.jsonl
    generated-sessions-001.jsonl
  checkpoints/
    ocpp_v1/
```

Only the incoming directory is read. The separate checkpoint directory stores the stream's progress so later runs know which files have already been processed. The managed Volume needs no separate cloud storage credentials for this demo. Receiving external GCS files is future work. See [Unity Catalog Volumes](https://docs.databricks.com/aws/en/volumes) and [Auto Loader production guidance](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production).

`publish_ingestion_demo` accepts the job parameter `batch`, which defaults to `batch_001`; the only other supported value is `batch_002`. Each committed test file has three event envelopes, one per line, for a synthetic charging session. An envelope contains `schema_version`, `run_id`, `event_id`, `charging_station_id` and a `payload` holding the original event JSON as a string. The second file uses a different session and identifiers.

The [session generator](#generate-new-charging-sessions) writes new files into the same incoming directory. It uses the same envelope format and adds its settings and a fixed tariff as metadata. Auto Loader keeps that metadata as part of the original text line. The generated evaluator validates it before calculating billing results.

The publisher accepts only these two files, with a 64 KiB size limit. It keeps their payload text unchanged and uses LF line endings. It checks the contents and size before and after publishing. Publishing an identical existing file changes nothing; conflicting contents are rejected. It never overwrites a published file, and storage or permission errors fail the task.

`ingest_ocpp_files` creates `workspace.chargeassert_dev_bronze.ocpp_events_landing` if needed. Auto Loader reads existing and new `.jsonl` files from the incoming directory with `cloudFiles.format = text`. The source schema is explicitly `value STRING`, with no partition columns or inferred JSON schema. Processing overwritten files is disabled.

The stream appends rows to Delta using `AvailableNow` and the saved `checkpoints/ocpp_v1/` checkpoint. It waits for ingestion to finish, reports stream errors if any occur, then stops. There is no schedule, continuous loop or always-running service. The job allows one run at a time so it does not intentionally start two streams using the same checkpoint.

Before calling Spark, configuration checks require the source to be `/Volumes/<catalog>/chargeassert_dev_bronze/ocpp_ingestion/incoming`, the checkpoint to be the separate `checkpoints/ocpp_v1/` directory and the destination to be that catalog's `chargeassert_dev_bronze.ocpp_events_landing`. Mismatched paths or table names are rejected.

`resources/ingestion.yml` defines the Volume and jobs. `notebooks/ocpp_ingestion_demo.py` publishes the files, called by the job notebook `notebooks/publish_ingestion_demo.py`. `notebooks/ocpp_file_ingestion.py` creates the landing table and runs the stream, called by `notebooks/ingest_ocpp_files.py`. The bundle explicitly uploads the committed `.jsonl` fixtures; `.gitattributes` keeps their line endings as LF across checkouts. The Volume and all three layer schemas use `prevent_destroy` to protect them from deletion through the bundle; this does not prevent every possible way of deleting them.

| Landing column | Type | Meaning |
| --- | --- | --- |
| `raw_record` | STRING | Original UTF-8 text line, excluding its line delimiter; JSON is not parsed and reserialized before storage |
| `record_hash` | STRING | SHA-256 of `raw_record`; evidence fingerprint, not an enforced unique key |
| `source_file_path` | STRING | Source path reported by the file reader |
| `source_file_name` | STRING | Source basename, such as `batch_001.jsonl` |
| `source_file_modification_time` | TIMESTAMP | Source modification time reported by the file reader |
| `ingested_at` | TIMESTAMP | Time the record was written through the ingestion query |

The source columns come from Databricks' [file metadata fields](https://docs.databricks.com/aws/en/ingestion/file-metadata-column); only the required fields are selected. `AvailableNow` is a supported trigger for [serverless jobs](https://docs.databricks.com/aws/en/compute/serverless/limitations).

Reusing the same checkpoint and Delta table preserves file-processing progress across runs. It does **not** remove a repeated business event that arrives in a different file. The landing table keeps the source evidence before business checks, including malformed JSON as raw text.

The generated evaluator now validates sessions-v1 envelopes, checks the intended session list, removes identical event copies for the selected batch and continues through separate Silver/Gold tables. Broader input schemas, a separate store for rejected records, late-event rules and safe backfills remain. The fixed seven-scenario billing job and its execution tracking stay separate.

#### Deploy and demonstrate 3 → 3 → 6 → 6

From the repository root in the authenticated Databricks terminal, run each command after the previous one succeeds:

```bash
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev publish_ingestion_demo --params batch=batch_001
databricks bundle run -t dev ingest_ocpp_files
```

Deployment creates the Volume and the job definitions. The publisher creates the first source file; ingestion creates the landing table and processes it. No run of `create_tables` is required for this separate demonstration. Wait for each job to report success. In Databricks SQL Editor, run:

```sql
SELECT source_file_name, COUNT(*) AS landed_rows
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
WHERE source_file_name IN ('batch_001.jsonl', 'batch_002.jsonl')
GROUP BY source_file_name
ORDER BY source_file_name;
```

Expect `batch_001.jsonl / 3`. Repeat ingestion and run the same SQL again; it should still show three rows:

```bash
databricks bundle run -t dev ingest_ocpp_files
```

Now publish the second file and ingest it:

```bash
databricks bundle run -t dev publish_ingestion_demo --params batch=batch_002
databricks bundle run -t dev ingest_ocpp_files
```

Expect `batch_001.jsonl / 3` and `batch_002.jsonl / 3`, six rows in total. Repeat ingestion again and verify the counts stay the same:

```bash
databricks bundle run -t dev ingest_ocpp_files
```

| Step | New sample rows | Total sample rows | Sample files |
| --- | ---: | ---: | ---: |
| Publish batch 1, ingest | 3 | 3 | 1 |
| Ingest again | 0 | 3 | 1 |
| Publish batch 2, ingest | 3 | 6 | 2 |
| Ingest again | 0 | 6 | 2 |

These counts describe only `batch_001.jsonl` and `batch_002.jsonl`; the query filters out generated batches. They assume this is the first time the two sample files are published and loaded. If both files already exist, the first ingestion processes both. If both are already loaded, repeating the sequence leaves six sample rows. Do not delete or overwrite source files, checkpoint state or the destination table to make the demo look new. The checkpoint belongs to this source and destination; a safe reset or replay procedure still needs to be designed.

Run [the read-only verification SQL](../sql/13_check_ocpp_ingestion.sql) after each successful ingestion. It checks total and per-file row counts, source metadata, raw-record hashes and repeated `(source_file_path, record_hash)` pairs. It also displays the original envelopes. For these test files, there must be zero invalid hashes, missing metadata values or repeated pairs. If no new file arrives, row counts and existing ingestion timestamps must stay unchanged. The default catalog is `workspace`; change the catalog in the SQL table names if deploying with another `catalog_name`.

These jobs need serverless notebook/job compute, Unity Catalog access and read/write access to the Volume. Check their outcomes in their own Databricks job runs. They do not write to the billing test job's `job_execution` or `execution_verdict` tables. The fifteen-task `create_tables` job can still run independently. Local checks cannot prove how checkpoints or serverless compute behave in the workspace. Deployment, the four-step demo and recovery after interrupted ingestion still need testing in Databricks.

### Generate new charging sessions

`generate_sessions` creates a batch of synthetic sessions, saves one JSONL file in the existing incoming directory, then stops. Each session has its own ID and three OCPP-shaped events: `Started`, `Updated` and `Ended`. Start times, duration and positive energy usage vary. All sessions carry the same EUR 0.45/kWh tariff setting.

The seed makes the variation repeatable. The same batch ID, count and seed produce the same file. This is test data, not real charging activity. The generator itself does not calculate a bill or create a CDR. After ingestion, `evaluate_generated_batch` validates the batch, runs both billing mocks and checks their outputs through the separate generated Silver and Gold tables.

| Setting | Default | Meaning |
| --- | --- | --- |
| `batch_id` | `sessions-001` | Identifies one saved batch. Use 1–32 lowercase letters, digits, hyphens or underscores, starting with a letter. Use a new ID for a new batch or changed settings. |
| `session_count` | `2` | Number of sessions, from 1 to 1,000. Each produces three event lines. |
| `seed` | `42` | Repeatable random seed, from 0 to 4,294,967,295. |

For `batch_id=sessions-repaired-001`, the file is `generated-sessions-repaired-001.jsonl` and every envelope has `run_id=generated-v1-sessions-repaired-001`. The envelope also records the generator version, batch ID, seed, session count and tariff metadata. A generated `run_id` identifies its input batch; it is not an execution of the separate billing checks.

#### Deploy, generate and check two sessions

Run each command only after the previous one succeeds:

```bash
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev generate_sessions --params batch_id=sessions-repaired-001,session_count=2,seed=42
databricks bundle run -t dev ingest_ocpp_files
```

In Databricks SQL Editor, check this batch only:

```sql
SELECT
  COUNT(*) AS event_rows,
  COUNT(DISTINCT get_json_object(
    get_json_object(raw_record, '$.payload'),
    '$.transactionInfo.transactionId'
  )) AS sessions
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
WHERE get_json_object(raw_record, '$.run_id') = 'generated-v1-sessions-repaired-001';
```

The [generated-session checks](../sql/14_check_generated_sessions.sql) also show event details and verify hashes. Set their `batch_id` SQL parameter to `sessions-repaired-001`.

Expect **6 event rows and 2 sessions**. Repeat the same generator command and ingestion command, then run the SQL again. It should still return **6 and 2**. The publisher leaves an identical file unchanged; the ingestion checkpoint remembers that the file was already processed.

For two more sessions, use a new batch ID:

```bash
databricks bundle run -t dev generate_sessions --params batch_id=sessions-repaired-002,session_count=2,seed=42
databricks bundle run -t dev ingest_ocpp_files
```

Change the SQL filter to `generated-v1-sessions-repaired-002`: expect another **6 rows and 2 sessions**. The first batch stays unchanged. To try 100 sessions later, use a fresh ID such as `sessions-100-001` with `session_count=100`; expect 300 event rows for that batch. The 1,000-session limit keeps this first version small. No throughput or cost benchmark has been measured yet.

#### Safe reruns and current limits

- Reusing a batch ID with changed settings is rejected. Use a new ID; do not overwrite the old file or reset the checkpoint.
- `combined_ingestion` runs the existing generator job and then the existing ingestion job, forwarding the selected batch settings. It reuses the ingestion job's concurrency limit rather than starting another notebook against the same checkpoint. Run other publishing and ingestion commands **one after the other**. `dbutils.fs.put` is not an atomic handoff to Auto Loader, so ingestion must not read while either publisher is writing. Each job allows one run at a time, but that does not prevent overlap between different jobs or manual writers.
- If generation fails, **do not start ingestion**. Rerun the same batch with the same settings. If it reports a partial or conflicting file, stop and investigate; the generator will not overwrite it. A safe cleanup and recovery procedure is still future work.
- The job has no schedule or continuous loop. It does not need a pause button: it stops after one batch. Scheduled generation and pause/resume controls can be added later.
- Auto Loader reads all available incoming files, not just the batch in the command you ran. Filter checks by the generated `run_id` so older sample files do not change the expected counts.
- Local tests check generated data and publishing behavior with substitutes for storage. The six-event landing count, the **6 → 6** repeat check and recovery still need direct verification in Databricks. Generation and ingestion do not write billing results. The evaluator below records those results in its own execution tables.


### Evaluate generated batches through Gold

The `evaluate_generated_batch` job completes the path from landed events to billing results. It reads **one selected batch**, validates all declared sessions, runs baseline and candidate billing mocks, calculates the expected charges separately in SQL, and saves the checks and verdict for that execution.

Its two job parameters are `batch_id` (default `sessions-repaired-001`) and `candidate_mode` (`healthy` by default, or `amount_error`). It allows one run at a time and stops when the batch is finished. It does not schedule more work or modify the source files, landing rows or Auto Loader checkpoint.

For the short test guide, simple Gold query and recorded result, see [Generated sessions](03-generated-sessions.md).

#### Fresh batch, healthy result, deliberate error

Run these commands from the updated repository after authentication. Wait for each command to succeed:

```bash
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev combined_ingestion --params batch_id=sessions-repaired-001,session_count=2,seed=42
databricks bundle run -t dev evaluate_generated_batch --params batch_id=sessions-repaired-001,candidate_mode=healthy
```

The last two commands do the following:

- **Create and load sessions:** `combined_ingestion` generates two simulated charging sessions and uses Auto Loader to load their six events into Bronze. `seed=42` makes the data repeatable.
- **Check correct billing:** `evaluate_generated_batch` with `candidate_mode=healthy` calculates the expected charges, compares both simulated billing versions and saves the checks in Gold. Expect **28 PASS checks**, with baseline, candidate and overall PASS.

**Test whether bad billing is caught.** Use the same sessions with deliberately wrong candidate bills:

```bash
databricks bundle run -t dev evaluate_generated_batch --params batch_id=sessions-repaired-001,candidate_mode=amount_error
```

This adds EUR 0.87 to each candidate bill. Expect **26 PASS / 2 FAIL / 0 BLOCKED**: one incorrect charge caught per session. Baseline stays PASS; candidate and overall become FAIL. Only the two candidate `amount_match` checks should fail. The job itself should succeed because it detected and saved the errors.

Each command runs once and stops. Nothing runs continuously.

Run the healthy evaluation again:

```bash
databricks bundle run -t dev evaluate_generated_batch --params batch_id=sessions-repaired-001,candidate_mode=healthy
```

Expect 28 PASS checks under a new execution ID. The healthy session and billing rows keep the same keys; the new attempt adds its own execution record and snapshot. The earlier faulty verdict remains available under its own evaluation run ID.

#### Read the exact generated execution

The evaluator prints its `execution_id`, `run_id`, session count, assertion count and financial verdicts. You can also find the matching attempt in SQL:

```sql
SELECT execution_id, batch_id, candidate_mode, run_id,
       started_at, finished_at, status, reason
FROM workspace.chargeassert_dev_bronze.generated_job_execution
WHERE batch_id = 'sessions-repaired-001'
ORDER BY started_at DESC;
```

Use the ID from the attempt you just ran with [sql/15_check_generated_billing.sql](../sql/15_check_generated_billing.sql). Set its only parameter, `execution_id`, to that exact value. The first query shows job status, financial verdicts, session/check counts and the reason; the second summarizes the saved checks by session and release. Missing, failed or incomplete attempts return BLOCKED. Confirm that the Databricks job itself finished successfully as well. A missing, failed or incomplete execution must not borrow a saved result from another attempt.

For a current-table investigation after that check, use the returned `run_id`:

```sql
SELECT release_role, session_id, assertion_id,
       expected_value, actual_value, difference, status
FROM workspace.chargeassert_dev_gold.generated_assertion_result
WHERE run_id = :run_id
ORDER BY release_role, session_id, assertion_id;
```

`generated_release_verdict` and `generated_assertion_result` describe the current evaluation rows. The immutable JSON in `generated_execution_verdict` is the saved historical result for one attempt. Never substitute a recent healthy row when the requested execution failed.

#### The thirteen generated tables

All names below have the `generated_` prefix and use the existing Bronze, Silver and Gold schemas. They do not replace or append generated sessions to the original demo tables.

| Layer | Table | Row key and purpose | Two-session healthy result |
| --- | --- | --- | ---: |
| Bronze | `generated_run_manifest` | `run_id`: input and billing-code fingerprints for this evaluation | 1 |
| Bronze | `generated_input_session` | `(run_id, session_id)`: complete intended session list and tariff link | 2 |
| Bronze | `generated_ocpp_transaction_events_raw` | `(run_id, event_id)`: original event payload, hash and routing fields | 6 |
| Bronze | `generated_tariffs_raw` | `(run_id, tariff_id)`: preserved EUR 0.45/kWh tariff payload and hash | 1 |
| Bronze | `generated_ocpi_cdrs_raw` | `(run_id, release_role, payload_hash)`: complete baseline/candidate mock CDR evidence | 4 |
| Bronze | `generated_job_execution` | `execution_id`: RUNNING, SUCCEEDED or FAILED attempt and its reason | 1 per attempt |
| Silver | `generated_session_lifecycle` | `(run_id, session_id)`: start/end timestamps and meter readings | 2 |
| Silver | `generated_tariff_history` | `(run_id, tariff_id, valid_from)`: validated rate and period | 1 |
| Silver | `generated_expected_ledger` | `(run_id, session_id)`: independent SQL energy and charge | 2 |
| Silver | `generated_actual_ledger` | Run, role, owner and CDR ID: normalized reported usage and charge | 4 |
| Gold | `generated_assertion_result` | Run, role, session and rule: expected/actual values and result | 28 |
| Gold | `generated_release_verdict` | `run_id`: baseline, candidate and overall billing decisions | 1 |
| Gold | `generated_execution_verdict` | `execution_id`: full verdict and all checks saved as JSON | 1 per successful attempt |

Counts for session and billing tables apply to one evaluation `run_id`. The faulty mode has a different run ID and keeps its own rows. Execution history grows when another full job runs; it is not supposed to stay at one row across attempts.

#### Input checks, calculation and evidence

- **Complete input first.** The evaluator checks the whole-line hash, JSON shape, schema/generator version, matching batch settings, station/session/event identities, and all declared sessions. Every session needs one Started, Updated and Ended event with sequences 0, 1 and 2, increasing timestamps and nondecreasing nonnegative Wh readings. Unsupported units, meter resets, conflicting metadata or missing sessions stop the evaluation.
- **Repeated delivery is checked.** Identical event envelopes with the same event ID can be collapsed for this selected batch, while landing keeps every received line. Conflicting versions of the same event fail. This is a bounded rule for `sessions-v1`, not general retry or late-arrival handling for external devices.
- **Bills are separate from expectations.** The Python mock creates one complete final CDR per session and release, including times, energy, duration, tariff and amount. It uses decimal arithmetic, rounds amounts HALF_UP to cents, and rounds duration to six decimal hours. Silver SQL independently computes the expectation from the validated meter readings and tariff. It never copies the candidate amount.
- **Every session must be priced.** `generated_input_session` drives the expected calculation. Missing or ambiguous session/tariff rows fail instead of producing a smaller ledger. Generated SQL reads only the selected run and uses separate generated tables; the original job remains limited to its seven fixtures.
- **Identity follows the evidence.** The source batch ID is `generated-v1-<batch_id>`. The billing evaluation uses `generated-check-v1-<batch_id>-<candidate_mode>-<hash>`, where the suffix binds input contents and pipeline code. A changed mode, input or code gets separate evidence. Manifest SHA fields fingerprint the mock module and behavior; they are not tested Git release commits. The tariff hash covers its actual payload.
- **Capture checks coverage again.** It requires exactly one matching verdict and every session × two releases × seven rules, without missing, duplicate or extra checks. Counts, statuses and manifest fingerprints must agree. Financial FAIL or BLOCKED checks can be saved as valid evidence; they never become a financial PASS because the job completed.
- **Attempts remain separate.** `generated_job_execution` records start, finish, status and failure reason. A successful attempt saves one immutable `generated_execution_verdict`, including input/code hashes, session/check counts and complete JSON evidence. A failed attempt remains failed. An interrupted job can leave RUNNING if it cannot finish its status write. Start a new complete job after a failure; repairs and reuse of an execution ID are rejected.

The source files are [generated_batch.py](../notebooks/generated_batch.py) for validation and mock billing, [generated_pipeline.py](../notebooks/generated_pipeline.py) for Databricks execution and evidence writes, [generated_capture.py](../notebooks/generated_capture.py) for snapshot checks, and [sql/generated/](../sql/generated/) for the independent transformations. The wrapper is [evaluate_generated_batch.py](../notebooks/evaluate_generated_batch.py); the job is defined in [generated_billing.yml](../resources/generated_billing.yml).

#### Existing files and remaining limits

The earlier extension added tariff and incomplete candidate-CDR lines to the event-only generator without changing its version. Those mixed files are rejected, not silently repaired or relabeled. Keep the original file and landed rows. Use a fresh batch ID such as `sessions-repaired-001` with the repaired generator. If that ID has already been used with different bytes, choose another fresh ID.

Do not delete files, reset checkpoints, drop tables or overwrite earlier billing evidence to make this test pass. Old generated rows written into the original fixture tables are also kept; the original job now processes only its seven registered fixture runs.

This version checks one bounded batch of up to 1,000 synthetic sessions, with at most 10,000 received lines including copies. It still uses mock billing, a flat EUR energy tariff and the supported final-CDR subset. Structured quarantine, arbitrary input schemas, late-event policies, external release replay and recovery after interrupted writes need further work. Separate generated tables and single-run jobs do not stop manual or external writes to those tables.

Local tests cover generation, validation, rounding, independent SQL comparisons, result capture and failure handling. The two-session healthy Gold result is confirmed in Databricks. Its job completion record, deliberate-error result, healthy rerun, stable counts and recovery after interruption still need checking.

## Silver — produce trusted business records

Silver checks the raw evidence, removes repeated copies and turns it into consistent session and billing records. It keeps the expected charge separate from the amount each release reports.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `session_lifecycle` | Implemented, happy path only | One logical charging session in one run | `run_id`, `session_id`, `started_at`, `ended_at`, `meter_start_wh`, `meter_end_wh`, `status` |
| `tariff_history` | Implemented; smoke join verified in Databricks | One effective tariff period in one run | `run_id`, `tariff_id`, `valid_from`, `valid_to`, `currency`, `price_components`, `source_payload_hash` |
| `expected_ledger` | Implemented for seven fixture sessions; original five checked through Gold in Databricks, missing-CDR pair pending | The independently calculated charge for one session | `run_id`, `session_id`, `expected_energy_kwh`, `price_per_kwh`, `expected_amount_unrounded`, `expected_amount`, `currency`, `tariff_id`, `tariff_valid_from`, `tariff_payload_hash` |
| `actual_ledger` | Implemented for the final-CDR subset; Gold smoke checks verified | One normalized CDR returned by either release | `run_id`, `release_role`, `country_code`, `party_id`, `session_id`, `cdr_id`, `cdr_type`, `started_at`, `ended_at`, `actual_energy_kwh`, `actual_duration_hours`, `actual_amount`, `currency`, `tariff_id`, `source_payload_hashes` |

`actual_ledger` keeps different CDR IDs for the same session and release. It removes repeated copies of the same payload, but keeps two different financial records for one billable session so Gold can flag the duplicate billing record.

### `session_lifecycle` contract

- Source: Bronze `ocpp_transaction_events_raw`; destination: `workspace.chargeassert_dev_silver.session_lifecycle`.
- Logical key: `(run_id, session_id)`, with OCPP `transaction_id` used as `session_id`. The charging station ID is not part of this key yet.
- Group each run's events by transaction. Use the earliest `Started` timestamp as `started_at` and the latest `Ended` timestamp as `ended_at`.
- Read `meterValue[0].sampledValue[0].value` from the start and end payloads. For each event type, take `MAX` of the extracted strings and then cast the result to `BIGINT` as Wh. With conflicting start or end events, these readings may not belong to the selected timestamps. Selecting the correct meter sample, measurement type, unit and multiplier still needs implementation.
- Mark a session `Completed` if it has any `Ended` event; otherwise mark it `In Progress`. `Updated` events remain available in Bronze but do not contribute to these boundary readings or timestamps.
- Reruns update the same logical session row or insert it if it is new. The smoke check requires one completed `txn-smoke-v1` session in `smoke-run-v1`, with meter readings of 100000 Wh and 112500 Wh.

This is a happy-path summary, not full event validation. It does not yet resolve conflicting events, retries, sequence errors, late arrivals or meter resets. Grouping rows into a session is not the same as checking those cases. `expected_ledger` adds checks on completion, timestamps and meter readings before it prices a session.

### `tariff_history` contract

- Source: `workspace.chargeassert_dev_bronze.tariffs_raw`.
- Destination: `workspace.chargeassert_dev_silver.tariff_history`.
- Logical key: `(run_id, tariff_id, valid_from)`. The same tariff ID can appear in another run without mixing the two runs' tariff records.
- A tariff applies from `valid_from` up to, but not including, `valid_to`: `[valid_from, valid_to)`. A null end means there is no end date. One period can start exactly when another ends. Conflicting or overlapping periods for the same run and tariff fail the task.
- `price_components` is `ARRAY<STRUCT<type: STRING, price: DECIMAL(18,6), step_size: INT>>`. The price is stored as a decimal, not a floating-point number.
- The supported tariff has one unrestricted `ENERGY` component in EUR, a price of zero or more and `step_size = 1`. VAT, price bounds, extra components/elements and restrictions are rejected. Dates come from the validity columns. Payload-level `start_date_time`/`end_date_time` are rejected until the code can check them against those columns.
- Validation fails for malformed payloads, missing required values, invalid periods, disagreement between metadata and payload, or invalid payload hashes. Exact duplicate records are removed before the merge.
- The source payload hash links the Silver row back to its Bronze evidence. Rerunning the merge updates the same logical period instead of adding another copy. Bronze remains the source of truth.
- The smoke check expects one `tariff-smoke-v1` period at `0.450000 EUR/kWh`, linked to the matching Bronze evidence. This step does not calculate a session charge or choose a billing rounding rule.

Silver can store several periods for a tariff. The current Bronze fixture merge matches only `(run_id, tariff_id)`, so ingestion still needs a version or period key before it can load several periods for the same tariff ID in one run.

### `expected_ledger` contract

- Sources: Silver `session_lifecycle` and `tariff_history`. The calculation does not read baseline or candidate billing outputs.
- Destination: `workspace.chargeassert_dev_silver.expected_ledger`; logical key: `(run_id, session_id)`.
- A fixed mapping prices `txn-smoke-v1` against `tariff-smoke-v1` separately in `smoke-run-v1`, `amount-bad-v1`, `amount-fixed-v1`, `mock-amount-bad-v1`, `mock-amount-fixed-v1`, `mock-missing-cdr-bad-v1` and `mock-missing-cdr-fixed-v1`. Each required session and tariff match is checked on its own. Duplicate rows in one run cannot hide a missing run.
- A missing candidate CDR does not remove the session's expected charge. Other sessions are outside this fixture mapping; a general scenario registry is still needed.
- Each calculation requires one completed session with valid start/end timestamps. Meter readings must be zero or more, and the end reading cannot be lower than the start reading. Exactly one matching tariff must apply at the session start. Missing or ambiguous matches fail the task.
- The supported tariff has one EUR ENERGY component with `step_size = 1`. The full session must fit within the selected tariff period. Sessions crossing a tariff boundary fail until pricing across periods is implemented.
- Energy is `(meter_end_wh - meter_start_wh) / 1000`, stored as `DECIMAL(18,6)`. Price is `DECIMAL(18,6)` and their unrounded product is retained as `DECIMAL(37,12)`.
- Round the session total once using **HALF_UP to two decimal places** and store `expected_amount` as `DECIMAL(18,2)`. This is the rounding rule chosen for the synthetic MVP; operators may use different rules. Databricks [`round(amount, 2)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/round) uses HALF_UP.
- Keep the selected tariff ID, period start, payload hash, currency and rate so the charge can be explained. Reruns update the same logical ledger row.
- The fixture check requires seven independently calculated rows. Each must show 12.5 kWh at EUR 0.45/kWh, EUR 5.625 before rounding and **EUR 5.63 after rounding**. Handling different meter formats and meter resets still needs more session validation work.

### `actual_ledger` contract

- Source: Bronze `ocpi_cdrs_raw`; destination: `workspace.chargeassert_dev_silver.actual_ledger`. The task waits for raw CDR loading, the amount fixtures and the mock generator. It reads only raw CDRs. It does not replace reported values with values from the expected ledger, session evidence or input tariffs.
- Logical key: `(run_id, release_role, country_code, party_id, cdr_id)`. Owner country/party use uppercase and CDR/tariff IDs use lowercase so letter case does not change their identity. The session ID stays unchanged for the existing OCPP mapping.
- Parse the original JSON into defined field types. Check payload integrity, run/release identity, owner/CDR/session IDs, timestamps, currency format and required numeric fields, which must be zero or more. Invalid or unsupported records fail the task; the original evidence stays in Bronze.
- The first supported records are final, non-credit CDRs with one charging period that names a tariff. Energy, duration and amount excluding VAT must use plain decimal numbers with at most six decimal places. More precision, scientific notation, credit CDRs and multiple charging periods need extra implementation. They are rejected so the task does not silently round values or process only part of a record.
- `actual_energy_kwh`, `actual_duration_hours` and `actual_amount` are `DECIMAL(18,6)`, taken from `total_energy`, `total_time` and `total_cost.excl_vat`. A reported EUR 5.625 stays 5.625; this table applies no currency rounding. The reported duration is stored separately from the timestamps.
- Different CDR IDs for one session stay separate. Records with the same identity and equivalent normalized billing values become one row. All distinct `source_payload_hashes` are kept in sorted order, so the full original payloads, including fields not modeled here, remain available.
- Conflicting normalized billing values for the same record identity raise an error naming the run/release/owner/CDR. The task does not choose a version, and all raw versions remain available. Reporting this failure as a Gold result is still to be implemented.
- A reported amount, energy, duration, currency or tariff can differ from the independent expectation and still be a valid Silver record. Gold checks whether those values are correct. The fixed smoke check separately verifies two healthy rows (baseline/candidate), each reporting 12.5 kWh, one hour and EUR 5.63.
- Reruns merge by the logical CDR identity. Both ledgers assume the source evidence is kept. Handling deleted or withdrawn source records, and checking every protocol rule, remain outside this first implementation.

### Local checks and their limits

Run the local regression checks with:

```powershell
python -B -m unittest discover -s tests -v
```

The tests cover several parts of the billing path:

- **SQL transformations and job dependencies.** Tests run the project's fixture-source, lifecycle, expected-charge, normalization, assertion and verdict SELECT queries. SQLite adapters stand in for parsed fields, timestamps, arrays/JSON and HALF_UP rounding. Cases cover keeping runs/releases separate, repeated or equivalent deliveries, distinct CDRs, conflicting or invalid records, fractions of a cent, missing/duplicate CDRs, incorrect billing values, missing expected values and evidence.
- **Paired fixtures and Python mocks.** Tests follow the seeded responses through the independent charge calculation and Gold decisions. They check that inputs stay unchanged, reject missing or ambiguous inputs to the expected calculation, and simulate insert-only keys when testing repeat loading. Python mock tests separately check the executable calculation and generated outputs without Databricks.
- **Verdict coverage.** Tests cover an entire missing release or session, duplicate checks taking the place of missing checks, unsupported rules/roles/statuses, empty runs, and missing or duplicate manifests.
- **File ingestion and publishing.** Tests check that raw source lines are kept exactly, including malformed JSON, whitespace and Unicode; hashes and metadata are selected correctly; and repeated business events in different files are kept. They also check the fixed checkpoint, append mode, AvailableNow calls, rejected configurations and reported failures. Publisher tests cover stable file bytes, harmless repeats, refused overwrites, truncated files, competing creates, storage errors and notebook/bundle wiring. These use fakes and mocks; they do not prove real Auto Loader checkpointing or recovery.
- **Session generation.** Tests check repeatable batches, distinct session and event IDs, valid event order and meter readings, parameter limits, and safe publishing. These are local checks; the generated files still need to be loaded and inspected in Databricks.
- **Generated billing and capture.** Tests run the actual validation and decimal billing code, plus generated SQL through adapters. They check both release records, healthy/faulty amounts, missing sessions and CDRs, source isolation, and complete snapshots. Capture also rejects a plausible smaller PASS result when one declared session is absent, inconsistent counts, changed manifest fingerprints and incorrect status summaries. Runtime mocks check receipt and failure handling; Databricks remains the deployment and recovery test.

These checks do **not** run a Databricks notebook, `from_json`, Spark type analysis/decimal arithmetic, Delta DDL or a real `MERGE`. Running the job in the workspace is still required.

Execution tests cover the Python snapshot and completion checks, plus the SQL query used to check an execution, using all seven financial fixtures. This includes the missing-CDR run's valid FAIL/BLOCKED assertion snapshot. They check:

- A successful run, followed by a failure before Gold, followed by a fresh successful run.
- Old verdicts, conflicting attempts to change saved snapshots, missing or duplicate evidence, incomplete task states, rejected repairs and inconsistent PASS counts.
- The capture path through `track`, using a strict mock of the Spark calls. Both scenario filters must receive individual string IDs, unrelated runs must be excluded, and all seven snapshots containing 98 assertions must reach verification. This tests the Python API calls, not real Spark execution.

Earlier completed snapshots with five scenarios can still be read by their exact execution ID. One successful Databricks execution with seven snapshots has been confirmed. The deliberate A/B/C failure demonstration, interruptions and partial runs still need workspace verification.

## Gold — make the release decision

Gold turns the billing comparisons into findings a business team can act on. An amount mismatch can flag a potential customer overcharge. A missing billing record can flag a potentially unbilled session. The results show what failed and keep the evidence needed to investigate it and review a release.

Gold stores the detailed checks, current scenario verdicts and saved execution snapshots that the pipeline does not overwrite. It supports a human release decision today. The automated GitHub release check and a dashboard are future work; Gold does not yet report total customer overbilling or operator revenue loss.

| Table | Status | One row represents | Important fields |
| --- | --- | --- | --- |
| `assertion_result` | Implemented; 14 smoke PASS rows verified in Databricks | One rule checked for one session and release | `run_id`, `release_role`, `session_id`, `assertion_id`, `expected_value`, `actual_value`, `difference`, `status`, `severity`, `message`, `evidence` |
| `release_verdict` | Implemented for seven fixtures; original five outcomes verified in Databricks, missing-CDR pair pending | The current decision for one scenario run, including both release outcomes | `run_id`, manifest details, check counts, `baseline_verdict`, `candidate_verdict`, `verdict`, `reason`, `first_problem`, `evidence`, `evaluated_at` |
| `execution_verdict` | Implemented; seven captured scenario rows confirmed | A saved scenario verdict and full set of checks from one job execution, kept unchanged | `execution_id`, `run_id`, release verdicts, assertion counts, `reason`, `verdict_snapshot`, `assertions_snapshot` |

The future GitHub check must require all three of these:

- The current full Databricks job finished successfully.
- Exactly one `job_execution` row has `status = 'SUCCEEDED'` for that attempt.
- A matching `execution_verdict` row has `verdict = 'PASS'` for the requested scenario.

A failed or incomplete job, missing or duplicate registration/snapshot, `BLOCKED` check or financial `FAIL` must block the release. An older successful execution or the changeable `release_verdict` table must never supply a fallback PASS. A direct report of differences between baseline and candidate is still planned. The existing independent checks already catch a billing mistake shared by both releases, because each is checked against the calculated expectation (the oracle).

### `assertion_result` contract

- Sources: Silver `expected_ledger`, `actual_ledger` and `session_lifecycle`. Destination: `workspace.chargeassert_dev_gold.assertion_result`.
- Logical key: `(run_id, release_role, session_id, assertion_id)`. Each release is checked against the independent expected result. The same mistake in baseline and candidate fails both; the baseline never supplies the expected value.
- Every expected session and every completed lifecycle session gets checks for both releases, even when a release has no CDR. A session found only in reported CDRs also gets checks for the reporting release. A missing expectation becomes a failure instead of disappearing in a join.
- There are **seven rules per session/release**, listed below. `status` is `PASS`, `FAIL` or `BLOCKED`; all current rules have severity `ERROR`. `BLOCKED` means an earlier requirement was not met, so the comparison could not run. It never means pass. `release_verdict` rejects failed or blocked checks, missing required checks, and runs with zero assertions.
- `expected_value` and `actual_value` are strings so one schema can store both numbers and text. Numeric values are compared as decimals before conversion to strings. `difference` is `DECIMAL(38,6)` and means **actual minus expected** when the numeric values can be compared. Text and blocked checks have a null difference. Amount differences require matching currencies. These are differences for individual checks; they do not estimate total revenue loss or financial exposure.
- Compare energy and amount exactly at their stored precision. Do not round the reported amount: expected EUR 5.63 versus reported EUR 5.625 fails with a difference of -0.005000. The expected amount already has the configured cent rounding.
- Expected duration is the time between the session's start and end. It uses [`timestampdiff(MICROSECOND, started_at, ended_at)`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/timestampdiff), divided by 3,600,000,000 using decimals, then rounded HALF_UP to six decimal hours. Compare `actual_duration_hours` exactly against that value. This is the synthetic fixture's duration rule; it does not yet separate charging time from pauses or parking.
- A missing or duplicate CDR fails `final_cdr_count` and blocks the five value comparisons. The check does not choose one CDR or add duplicate amounts together to hide the problem. Missing, duplicate or invalid expected-ledger/lifecycle rows fail `oracle_available` and block the checks that depend on them.
- `evidence` is JSON containing the rule version, source row counts, selected tariff ID/period/hash/currency, session timestamps, and a sorted list of all actual CDR identities, reported values and Bronze payload hashes. Use the result's run/release/session keys to find Silver records, and `(run_id, release_role, payload_hash)` to find the original Bronze CDRs.
- The [Delta merge](https://docs.databricks.com/gcp/en/delta/merge) updates existing keys, inserts new keys and removes Gold results that are no longer in the complete current source set. This original table reflects the seven fixed scenarios; generated runs use `generated_assertion_result`. Neither current table is the permanent history of every evaluation. Identical inputs produce the same logical rows on rerun. The task does not change Bronze or Silver, and their existing limits around deleted sources still apply. The generated evaluator processes only its selected run; its scoped merge leaves other evaluation rows alone.
- The fixed healthy fixture must produce **14 PASS rows: seven baseline and seven candidate**. Financial FAIL/BLOCKED results in another run are stored without raising a SQL exception. A successful table-creation job does not mean a release passed its billing checks. Keep the healthy smoke run unchanged when testing faults: earlier fixture checks require its original values.

| `assertion_id` | What it checks |
| --- | --- |
| `oracle_available` | Exactly one expected ledger row and one valid completed lifecycle row are available. |
| `final_cdr_count` | Exactly one final financial CDR exists for the completed billable session; zero means missing, more than one means duplicate. |
| `energy_match` | Reported kWh equals independently expected kWh. |
| `duration_match` | Reported duration equals elapsed lifecycle hours rounded to six decimals. |
| `currency_match` | Reported currency equals the expected currency. |
| `tariff_match` | Reported tariff ID matches the selected tariff ID after case normalization. |
| `amount_match` | Reported amount excluding VAT equals the rounded expected amount in the same currency. |

This first implementation reads the supported final-CDR records from Silver. It does not yet compare releases directly, check the full reported tariff version/content, compare reported start/end timestamps, turn Silver parsing/conflict failures into Gold rows, or check HTTP retry and late-event traces. The original expected ledger prices seven explicitly mapped fixtures. The generated path uses its validated intended-session list and separate expected ledger. The GitHub release check is not implemented yet.

### `release_verdict` contract

- Sources: Bronze `run_manifest`, Gold `assertion_result`, and Silver `expected_ledger`, `session_lifecycle` and `actual_ledger`. Destination: `workspace.chargeassert_dev_gold.release_verdict`; logical key: `run_id`.
- Within the seven registered fixture runs, produce one row for every run found in any source table above. A manifest with no sessions/checks gets FAIL; data with no manifest also gets FAIL. Exactly one manifest is required. Copy `scenario_id`, `seed`, `baseline_sha`, `candidate_sha` and `tariff_hash` only when there is exactly one manifest. These fields stay null if the manifest is missing or duplicated.
- Canned-fixture SHAs are synthetic labels. Mock SHAs identify the applicable source modules and behavior by their fingerprints. Neither is a verified commit from a tested release's source code.
- Work out the required checks independently from Silver, using the same sessions as `assertion_result`. Expected-ledger and completed-lifecycle sessions require both release roles. Supported FINAL sessions found only in the actual ledger require their reporting role. Each session/release pair needs all seven registered rules. A missing session or release cannot pass simply because fewer assertion rows were produced.
- Each release must have a nonempty set of required checks, exactly one PASS row per required key, and no failed, blocked, duplicate, unexpected or invalid-status assertions. Both `baseline_verdict` and `candidate_verdict` must be PASS for the overall `verdict` to pass. An assertion with an unknown release role also fails the overall verdict, even when both known releases pass.
- Verdicts are `PASS` or `FAIL`. Blocked assertions stay in the counts and produce a FAIL verdict. A failed baseline fails the overall run even if the candidate passes. The same mistake in both releases does not make either correct.
- `reason` explains the highest-priority problem. `first_problem` is JSON containing the problem type and available run/release/session/assertion keys. The fixed priority order is: manifest error, empty coverage, missing check, duplicate check, unexpected check, invalid status, FAIL, then BLOCKED. Ties sort by release/session/assertion. This gives reviewers a starting point; it is **not the first difference in time**. For an existing check, use these keys to find its values, message and original evidence in `assertion_result`.
- `evidence` contains the rule version, manifest-row count, and separate `baseline`/`candidate` summaries with counts of required and observed checks and verdicts. Run-level totals also include assertions with unsupported release roles.
- The merge updates, inserts and removes current verdicts only within the seven registered fixture runs. Rows outside that inventory stay untouched. `evaluated_at` records the latest evaluation and changes on rerun. Identical inputs keep the same decision, counts and problem reference.
- The task runs after `create_assertion_result` with [`run_if: ALL_SUCCESS`](https://docs.databricks.com/gcp/en/jobs/run-if). A financial FAIL is stored as data. Fixture checks require the healthy smoke run to PASS, each bad amount candidate to FAIL with exactly one amount mismatch, the bad missing-CDR candidate to FAIL with one failed count and five blocked comparisons, and all corrected candidates to PASS. A job that correctly detects an intentional billing defect has done its job successfully.

| Count column | Meaning |
| --- | --- |
| `required_assertions` | Number of distinct required session/release/rule keys derived from Silver. |
| `passed_assertions`, `failed_assertions`, `blocked_assertions` | Stored assertion rows with each status, including duplicate or unexpected rows. |
| `missing_assertions` | Required keys with no stored assertion row. |
| `duplicate_assertion_keys` | Assertion keys with more than one stored row. |
| `unexpected_assertions` | Stored rows whose session/release/rule key is outside the required set. |
| `invalid_assertions` | Stored rows with null or unrecognized status. |

These counts can overlap. For example, a duplicate unexpected PASS can count as passed, duplicate and unexpected. Matching passed and required counts alone is never enough for PASS.

**Reading the right execution:** `release_verdict` can change, and `evaluated_at` alone does not prove that it belongs to the job attempt being reviewed. Use `job_execution` and `execution_verdict`, described below, so an earlier PASS cannot hide a failure before Gold. The original fixture path has no general intended-session registry. Generated batches now have that independent list in `generated_input_session`, and capture rejects a missing whole session. Full Silver/source versioning and protection from unrelated writers still need work.

The verdict gives reviewers a decision and counts they can trace back to the checks. Future work includes direct release comparison, finding the first difference in the event timeline, separate totals for customer overbilling and operator revenue loss, and estimates of wider financial exposure with stated assumptions. The current table does not fill these missing monetary measures with misleading zeros.

### `execution_verdict` contract

- Destination: `workspace.chargeassert_dev_gold.execution_verdict`; logical key: `(execution_id, run_id)`. The capture task saves the full scenario verdict in `verdict_snapshot` and all its assertion rows in `assertions_snapshot` as JSON. It also stores verdicts, counts and reasons in fields that can be queried directly. A later pipeline evaluation does not rewrite an earlier execution's evidence.
- Capture requires a valid `RUNNING` registration, repair count zero, all thirteen earlier tasks explicitly reporting `success`, seven unique scenario verdicts from the current evaluation, and complete matching assertion evidence. A skipped or excluded task does not qualify, even if the scheduler allows a later task to run.
- Snapshots are insert-only. Repeating an identical capture adds no duplicates; trying to reuse the same key with different evidence is rejected. A new normal execution produces seven snapshots containing ninety-eight assertion records in total: three deliberate financial FAIL cases and four passing cases. The missing-CDR snapshot includes its five `BLOCKED` assertions. Those expected test results do not make the job execution fail.
- A snapshot alone does not prove the job finished. The capture task can write data before it fails or is canceled. Before recording `SUCCEEDED`, the finish task independently checks that capture succeeded, every other required task succeeded, and the full set of snapshots exists. Readers must join the snapshot to that exact successful registration.
- Use [sql/12_check_execution.sql](../sql/12_check_execution.sql) to check an execution. Its parameters are `execution_id`, scenario `run_id`, `job_execution_table_name` and `execution_verdict_table_name`. It reads only the requested identifiers, returns one row even when registration is missing, and returns `BLOCKED` for incomplete, failed, duplicate or missing evidence.
- These checks assume fixture inputs are kept unchanged and only one execution of the configured job runs at a time (`max_concurrent_runs: 1`). They do not version every Silver row, prevent manual or external writes, or replace checking the Databricks job's final successful state. The future GitHub release check must account for these limits.

## Billing regression job and deployment

Run the billing checks with the bundle job `create_tables`. In the workspace, its name is `chargeassert_dev_create_tables`. It has fifteen tasks, shown below. The [session generator](#generate-new-charging-sessions) and two [file-ingestion jobs](#incremental-ocpp-file-ingestion) run separately.

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

Use Databricks CLI version `>= 0.295.0`, as required by `databricks.yml`, and sign in to the workspace used for the dev deployment. From the repository root, get the latest `dev` code, deploy it, then run the job:

```powershell
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev create_tables
```

Continue only after each command succeeds. The two Databricks commands do different jobs:

- `deploy` uploads the SQL, notebooks and Python helpers, then updates the bundle's resources.
- `run` registers a new execution, prepares the tables and test data, runs the mock once, checks all seven scenarios through Gold and saves the results and completion status.

SQL tasks use the configured `Serverless Starter Warehouse`. The mock and tracking notebooks use serverless job compute. Both must be available to the identity running the job.

`TERMINATED SUCCESS` means the deployed version of the job succeeded. Check that it contains **fifteen tasks**, including `begin_execution`, `capture_execution` and `finish_execution`. If any is missing, pull the updated `dev` branch and **deploy before running again**. A Git pull alone does not update the deployed job.

Normal runs load and check all seven scenarios automatically; no manual table inserts or parameter changes are needed. `fail_before_gold` defaults to `false`; use `true` only for the failure demonstration below. The generator and tracking tasks stop when the job finishes and run again only when another job is started.

See the [Databricks bundle command reference](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands).

### Capture failure recovery

If the calculation tasks succeed but `capture_execution` fails, open that task's **Output** to find the original error. `Required tasks did not all succeed: capture_execution=failed` tells you which task failed, but not why. A later `A failed execution cannot be promoted to SUCCEEDED` message means the finish task is keeping an already-failed attempt marked as failed.

The capture code now uses `.isin(*EXPECTED_RUN_IDS)` when reading verdicts and assertions. This passes each scenario ID separately; passing the whole tuple as one argument is unsupported by Spark. After the correction, execution `192226331898541:436138745571440:0` was confirmed as `SUCCEEDED`, with seven snapshots for seven different scenarios. The original failed task's full traceback was not available, so the exact original error was not confirmed.

After pulling and deploying the fix, start a **new complete job run**. Do not repair the failed attempt or change its status; a failed execution stays failed. Check that the new job succeeds, its own `job_execution` row says `SUCCEEDED`, and it has seven `execution_verdict` snapshots. Keep the failed attempt in the history.

### Check the requested execution

First, run this query in Databricks SQL and find the row matching the job and run IDs of the job you just started:

```sql
SELECT execution_id, status, reason
FROM workspace.chargeassert_dev_bronze.job_execution
ORDER BY started_at DESC
LIMIT 20;
```

Copy that exact `execution_id`. Do not switch to a different execution just because it succeeded. If this job has no row, build its `<job_id>:<job_run_id>:0` identifier from the Databricks run details and check it anyway. A missing row means the execution is incomplete; an older PASS cannot replace it.

Run [sql/12_check_execution.sql](../sql/12_check_execution.sql) in SQL Editor with these parameter values:

| Parameter | Value |
| --- | --- |
| `execution_id` | Exact identifier for the requested job attempt |
| `run_id` | `mock-missing-cdr-fixed-v1`, then `mock-missing-cdr-bad-v1` (or another intended fixture) |
| `job_execution_table_name` | `workspace.chargeassert_dev_bronze.job_execution` |
| `execution_verdict_table_name` | `workspace.chargeassert_dev_gold.execution_verdict` |

After a successful full job, use the same `execution_id` to check both missing-record scenarios:

| `run_id` | Execution status | Financial verdict | Passed / failed checks |
| --- | --- | --- | --- |
| `mock-missing-cdr-fixed-v1` | `SUCCEEDED` | `PASS` | 14 / 0 |
| `mock-missing-cdr-bad-v1` | `SUCCEEDED` | `FAIL` | 8 / 1 |

The bad scenario's snapshot also contains five blocked checks; the diagnostic query below shows that count. The amount pair still gives 13 passed / 1 failed for the bad case and 14 passed / 0 failed for the fixed case.

A failed or incomplete execution must return financial `BLOCKED`, with null release verdicts and counts. A missing execution also returns `execution_status = 'MISSING'`. Duplicate registration or snapshot rows block the result too.

The SQL below performs the same check using the default development table names. Set `:execution_id` and `:run_id` in SQL Editor:

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

This test uses three separate job runs:

1. Start a normal full job (A). Save its exact execution ID and check the fixed mock: execution `SUCCEEDED`, financial `PASS`.
2. Start another full job (B), asking it to fail just before the Gold checks:

   ```powershell
   databricks bundle run -t dev create_tables --params fail_before_gold=true
   ```

   The assertion task fails before changing Gold. Expect a CLI/job error, a `FAILED` record for B once the finish task runs, and no snapshots for B. Check **B's identifier**: it must return `BLOCKED`, even though A's saved snapshot and the current Gold PASS still exist. If the finish task is interrupted, `RUNNING` or `MISSING` must also block the result.
3. Start a new full job (C) with the default parameter:

   ```powershell
   databricks bundle run -t dev create_tables
   ```

   Expect a new successful execution with seven snapshots and the scenario outcomes described here. A's snapshots must stay unchanged, and B must stay failed or incomplete. Start a new full job; do not use Repair or run only selected tasks.

The failure parameter tests job handling without changing raw evidence or expected billing results. [Databricks `--params`](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#pass-job-parameters) applies only to the job run started by that command. This A/B/C demonstration still needs to be run on the deployed fifteen-task job in Databricks.

### Inspect the current diagnostic tables

The queries below help explain the test data. They read tables that later jobs can update, so they do **not** replace the exact-execution check above. After a successful full job, start with the expected ledger:

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

Expect one row for `txn-smoke-v1` / `tariff-smoke-v1`: 12.500000 kWh at EUR 0.450000/kWh, giving EUR 5.625000000000 before rounding and EUR 5.63 after rounding. Run the job again and check that there is still only one row. You can inspect the source tariff in `workspace.chargeassert_dev_silver.tariff_history`; its test period is `[2026-01-01T00:00:00Z, 2026-12-31T23:59:59Z)`.

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

Expect `baseline / PASS / 7` and `candidate / PASS / 7`. These 14 passing checks were confirmed on 2026-09-18. To see the source details behind a check, select its `evidence` field from the same table. Stable counts after rerunning still need to be verified in Databricks; local SQLite tests do not prove this.

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

Each summary should show seven required checks, seven passed checks, no problems and a PASS verdict. This healthy smoke result was confirmed on 2026-09-19.

Inspect the original amount scenarios that use saved billing responses:

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

The bad candidate must show **5.630000 expected / 6.500000 actual / +0.870000 / FAIL**. The corrected candidate must show **5.630000 / 5.630000 / 0.000000 / PASS**. Both baselines remain PASS. These verdicts and their 13/1 versus 14/0 check counts were confirmed in Databricks on 2026-09-20.

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

Expect four rows. Only `mock-amount-bad-v1 / candidate` should show **5.630000 expected / 6.500000 actual / +0.870000 / FAIL**. The other three rows should show **5.630000 / 5.630000 / 0.000000 / PASS**.

Query Bronze `ocpi_cdrs_raw` with these run IDs to see the generated `cdr-mock-v1` payloads and their hashes. This pair's verdicts and 13/1 versus 14/0 counts were confirmed on 2026-09-21. A later successful execution saved seven snapshots. The deliberate failure demonstration still needs to be run.

Inspect the **missing-CDR pair** after confirming that the exact execution completed successfully:

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

To display a zero for the missing record, start with the expected runs and roles, then count their matching CDRs:

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

Expect four rows: bad baseline **1**, bad candidate **0**, fixed baseline **1**, fixed candidate **1**. Grouping only the existing CDR rows would leave out the missing candidate instead of showing zero. These queries explain the current tables; use the exact-execution snapshot as the historical record. This pair still needs direct confirmation in Databricks.

Across the seven scenarios, expect these totals. If other data exists, filter to the seven scenario run IDs first.

| Table | Expected rows |
| --- | ---: |
| `run_manifest` | 7 |
| `ocpp_transaction_events_raw` | 21 |
| `tariffs_raw` | 7 |
| `ocpi_cdrs_raw` | 13 |
| `session_lifecycle` | 7 |
| `tariff_history` | 7 |
| `expected_ledger` | 7 |
| `actual_ledger` | 13 |
| `assertion_result` | 98 |
| `release_verdict` | 7 |

Run the full job again with the same code and inputs. These counts and raw payload hashes should stay the same, including zero candidate CDRs in the bad missing-record scenario. All three deliberately faulty runs must keep their FAIL verdicts. `evaluated_at` may change. If the job reports changed evidence, investigate the code or input change. An intentional change needs new versioned run IDs.

Execution history grows with each new job. A successful full job adds one `job_execution` row and seven `execution_verdict` snapshots containing 98 check results as JSON. Repeating the capture within the same attempt does not add duplicate snapshots. A failure before Gold adds an execution record but no verdict snapshots. Interrupted registration or completion leaves an incomplete outcome.

Earlier completed executions with five scenarios remain unchanged and can still be read using their exact IDs. Execution tracking does not change the ten scenario-table totals above.

## Remaining MVP work

The items below separate what already works from what still needs to be built or checked in Databricks.

| Workstream | Still missing |
| --- | --- |
| Repeatable inputs | The seeded generator and selected-batch evaluator have produced a healthy two-session Gold result. Verify repeat runs in Databricks, then add a broader versioned scenario registry and real baseline/candidate Git commit IDs. Generated evaluations bind input/code hashes and intended sessions; original saved-response runs keep synthetic SHA labels and the smoke tariff hash still covers an identifier. |
| Public data and file ingestion | Verify Auto Loader and interrupted-run recovery in Databricks. Add external GCS files, versioned ACN-Data charging behavior and French IRVE station data, and explain how they form synthetic inputs. The generated adapter validates sessions-v1 envelopes and handles identical event copies; quarantine, broader schema changes, late arrivals and safe backfills remain. Do not describe composed inputs as real French transactions or actual operator tariffs. |
| Replay and deliberate faults | Wrong-amount and missing-CDR mock pairs already use identical inputs; the missing-CDR results still need direct workspace verification. Add duplicate charges and the other required cases, then replay inputs against external releases over HTTP. Record events and retry attempts so the first difference can be found. Current mocks do not run real software release builds. |
| Session validation | Generated batches now check declared identities, complete event sequences, timestamps and nondecreasing Wh readings before billing. Broaden support for other meter measurements, units/multipliers, resets and external station/session identities. Define late, missing and conflicting-event policies beyond the bounded synthetic contract. |
| Tariff selection | Generated sessions now have explicit session/tariff links; the original seven mappings remain fixed. Add multiple tariff periods in Bronze and pricing across tariff boundaries. Extend pricing rules only when a scenario needs them. |
| Independent expected charge | The independent SQL calculation now covers every registered session in the selected generated batch, as well as the original fixtures. The healthy two-session Gold result is confirmed. Check deliberate errors and broaden supported scenarios, tariffs and meter formats while keeping the calculation separate from Python billing mocks. |
| Reported billing records | Read responses from external release tests and support more than the current final-CDR fields. Parsing, removal of equivalent copies and rejection of conflicting records already work. Still needed: Gold results for those conflicts, broader Session/CDR support and a general mapping between session IDs. Keep the original saved responses as regression test evidence. |
| Billing checks | The original healthy 14-check result and a generated two-session Gold result with 28 checks are confirmed in Databricks. Local tests also cover failures in the expected calculation, CDR count, energy, duration, currency, tariff ID and amount. Add direct baseline-versus-candidate checks, tariff-version evidence, checks that retries and late events do not change the correct result, and clear reports for failures in earlier tasks. |
| Verdicts and evidence | The original successful seven-snapshot execution is confirmed; inspect its financial snapshots and complete the A/B/C failure test. Generated capture now checks a complete session inventory and binds input/code hashes to an exact attempt. One healthy generated Gold result is confirmed; check its completion receipt and prove snapshots stay unchanged after reruns. Full source/Silver versioning, protection from unrelated writers, first-difference traces and business exposure summaries remain. |
| Estimated financial impact | Calculate the change in defect rate × assumed monthly sessions × assumed impact per affected session. Show every assumption and separate customer overcharges from potentially unbilled revenue. Label these numbers as estimates, never actual losses or proven savings. |
| GitHub automation | Add GitHub Actions that authenticate to Databricks, run the checks, retrieve the exact execution/scenario result and publish PASS/FAIL with evidence links. Configure that check as required for release approval. Require a successful Databricks job, a successful execution record and a passing snapshot; never use an older success instead. |
| Verification and demo | The generated healthy Gold result is confirmed: two sessions, 28 checks and all verdicts PASS. Confirm its completion receipt, then test 26 PASS / 2 FAIL with amount_error and a fresh healthy execution with 28 PASS. Check stable session counts and unchanged older snapshots. Also finish generated ingestion 6 → 6, sample files 3 → 3 → 6 → 6, missing-CDR snapshot checks, A/B/C failure and interrupted-run recovery. The PDF examples of 50,000 sessions and EUR 24,380 are not measured results. |
| Runtime access | Add explicit permissions when introducing a separate CI or runtime identity. Current development relies on schema ownership. |

### Six flagship scenarios

| Scenario | Required demonstration |
| --- | --- |
| Missing CDR | Implemented in the mock; direct workspace verification is pending. A completed billable session has no candidate final CDR. The record-count check fails, and comparisons that need that record are blocked. The independently expected charge shows the potentially unbilled value; a total revenue-impact report is still missing. |
| Duplicate charge | A retry creates two different CDR IDs for the same session. Keep and report both records. |
| Wrong energy | Meter order, a reset or incorrect units change the billed kWh. Detect the difference using checked source evidence. |
| Wrong tariff | Billing uses the wrong price or tariff version. Show the tariff and amount mismatch. |
| Retry failure | An HTTP failure causes a repeated request to change the result. Show the first retry where the behavior differs. |
| Late event | Events arrive out of order and change the final outcome. Check the result against the same logical charging session. |

## Next implementation step

1. **Finish the generated-session checks in Databricks.** The healthy two-session Gold result is confirmed. Check its completion record and six landed events, then test amount_error → healthy on the same batch. Expect 26 PASS / 2 FAIL → 28 PASS, with stable session counts and older results intact.
2. **Prove failure handling and recovery.** Test incomplete input, a failure before Gold and an interrupted attempt. Each requested failed or incomplete execution must return BLOCKED while older PASS evidence stays stored. Also complete ingestion repeat checks and design a review path for rejected records.
3. **Finish the original billing evidence checks.** Inspect the missing-CDR run's 8 PASS / 1 FAIL / 5 BLOCKED checks and its corrected 14 PASS result. Run the original A/B/C failure demonstration without repairing or rewriting earlier attempts.
4. **Broaden the release tests and measure them.** Add duplicate charges, wrong energy, wrong tariffs, retries and late events. Then add external release replay, documented public data, measured runtime/cost and GitHub checks tied to exact execution evidence.

The MVP does not include real card payments; bank, payment-provider (PSP), ERP or settlement connections; full OCPP/OCPI certification; production monitoring and recovery; support for every tariff, tax and currency; machine learning; or confidential operator data.

## Permissions status

- The deployment user was previously recorded as owning all three schemas. Check current permissions when deploying as another identity.
- The bundle does not yet grant access to another user, group, service principal or CI identity.
- A separate identity running the jobs needs `USE CATALOG` on `workspace`, plus `USE SCHEMA` and `CREATE TABLE` on the relevant schemas. It also needs `SELECT` and `MODIFY` on tables it reads or writes without owning them, and access to the job, SQL warehouse and serverless notebook/job compute.
- Creating the managed ingestion Volume needs `CREATE VOLUME` on the Bronze schema. A separate publisher or ingestion identity also needs `READ VOLUME` and `WRITE VOLUME` on `workspace.chargeassert_dev_bronze.ocpp_ingestion` for source files and checkpoints, along with catalog and schema access.
- An identity that only reads tables, including the landing table, needs `USE CATALOG`, `USE SCHEMA` and `SELECT`. It does not need direct Volume access to read landed rows.

```sql
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_bronze;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_silver;
SHOW GRANTS ON SCHEMA workspace.chargeassert_dev_gold;
```

When adding CI or collaborators, grant only the access each identity needs. All current test data is synthetic.
