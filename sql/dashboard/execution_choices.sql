-- The default includes the latest attempt even when it failed or is still running.
SELECT 'latest' AS execution_id
UNION
SELECT execution_id FROM chargeassert_dev_bronze.generated_job_execution;
