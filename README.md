# ChargeAssert

Financial release testing for EV charging.

ChargeAssert is an independent public portfolio project.

It preserves synthetic charging evidence in Bronze, derives trusted records and an independent charge calculation in Silver, then compares baseline and candidate billing results in Gold. The existing regression job evaluates seven fixed scenarios and retains immutable results for each job execution.

An independent file-ingestion demo now loads OCPP event envelopes from a managed Unity Catalog Volume into a Bronze landing table using Auto Loader. It processes available files and exits. This first ingestion slice preserves raw records and source metadata; it is not yet connected to the billing transformations.

## Run the incremental-ingestion demo

From the repository root in your authenticated Databricks terminal, run each command only after the previous one succeeds:

```bash
git switch dev
git pull --ff-only origin dev
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev publish_ingestion_demo --params batch=batch_001
databricks bundle run -t dev ingest_ocpp_files
```

In Databricks SQL Editor, check the first three landed records:

```sql
SELECT source_file_name, COUNT(*) AS landed_rows
FROM workspace.chargeassert_dev_bronze.ocpp_events_landing
GROUP BY source_file_name
ORDER BY source_file_name;
```

Run ingestion again: the same checkpoint must keep the total at **3**.

```bash
databricks bundle run -t dev ingest_ocpp_files
```

Publish the second file and ingest it: the total should become **6**, with three records per file. The final repeat should keep it at **6**.

```bash
databricks bundle run -t dev publish_ingestion_demo --params batch=batch_002
databricks bundle run -t dev ingest_ocpp_files
databricks bundle run -t dev ingest_ocpp_files
```

These counts assume a first demonstration with only the two supplied files. Repeating the complete demonstration after both files have landed leaves six rows; it does not reset storage. Keep published files unchanged and retain the checkpoint. File-processing progress does not deduplicate the same business event delivered in a different file.

The bundle creates the managed Volume; the ingestion job creates its landing table. The jobs require Unity Catalog and serverless notebook/job compute. Workspace verification of this new ingestion demo is pending. The existing billing regression is still run separately with `databricks bundle run -t dev create_tables`.

See [the implementation and runbook](docs/01-tables.md#incremental-ocpp-file-ingestion) and [the read-only verification queries](sql/13_check_ocpp_ingestion.sql) for metadata, hash and repeat-run checks, limitations and the next steps.
