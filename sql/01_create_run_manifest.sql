-- The IDENTIFIER clause safely converts the job parameter into a table name.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
  run_id STRING NOT NULL
    COMMENT 'Stable identifier for one deterministic test run.',
  scenario_id STRING NOT NULL
    COMMENT 'Scenario manifest replayed by this run.',
  seed BIGINT NOT NULL
    COMMENT 'Seed used to reproduce generated inputs.',
  baseline_sha STRING NOT NULL
    COMMENT 'Git commit SHA of the trusted baseline.',
  candidate_sha STRING NOT NULL
    COMMENT 'Git commit SHA of the candidate under test.',
  tariff_hash STRING NOT NULL
    COMMENT 'SHA-256 hash of the tariff inputs.',
  created_at TIMESTAMP NOT NULL
    COMMENT 'Timestamp when this run manifest was created.'
)
USING DELTA
COMMENT 'Immutable manifest for one reproducible ChargeAssert test run.'
TBLPROPERTIES (
  'chargeassert.layer' = 'bronze',
  'chargeassert.environment' = 'dev'
);

-- Insert one stable smoke-test row only once, even when the job is rerun.
MERGE INTO IDENTIFIER(:table_name) AS target
USING (
  SELECT
    'smoke-run-v1' AS run_id,
    'smoke-scenario-v1' AS scenario_id,
    CAST(42 AS BIGINT) AS seed,
    sha1('baseline-smoke') AS baseline_sha,
    sha1('candidate-smoke') AS candidate_sha,
    sha2('tariff-smoke-v1', 256) AS tariff_hash,
    CAST('2026-08-22T00:00:00Z' AS TIMESTAMP) AS created_at
) AS source
ON target.run_id = source.run_id
WHEN NOT MATCHED THEN INSERT (
  run_id,
  scenario_id,
  seed,
  baseline_sha,
  candidate_sha,
  tariff_hash,
  created_at
) VALUES (
  source.run_id,
  source.scenario_id,
  source.seed,
  source.baseline_sha,
  source.candidate_sha,
  source.tariff_hash,
  source.created_at
);

-- The task output should show exactly one row after every successful run.
SELECT *
FROM IDENTIFIER(:table_name)
WHERE run_id = 'smoke-run-v1';
