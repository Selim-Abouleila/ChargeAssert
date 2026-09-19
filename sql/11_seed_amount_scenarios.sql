-- Two immutable replays of the same smoke inputs. Only the bad candidate's
-- reported amount changes. These are synthetic responses, not release adapters.
-- Run after the four Bronze smoke loaders and before any Silver calculation.
-- Fail closed if the source fixture is missing, duplicated or has drifted.
SELECT assert_true(
  COUNT(*) = 1
    AND count_if(seed = 42 AND baseline_sha = sha1('baseline-smoke')) = 1,
  'Amount scenarios require one intact smoke run manifest.'
)
FROM IDENTIFIER(:run_manifest_table_name)
WHERE run_id = 'smoke-run-v1';

SELECT assert_true(
  COUNT(*) = 3
    AND COUNT(DISTINCT event_id) = 3
    AND count_if(
      transaction_id = 'txn-smoke-v1'
        AND charging_station_id = 'cs-smoke-001'
        AND payload_hash = sha2(payload, 256)
        AND get_json_object(payload, '$.transactionInfo.transactionId') = transaction_id
        AND get_json_object(payload, '$.eventType') = event_type
        AND CAST(get_json_object(payload, '$.seqNo') AS BIGINT) = sequence_number
    ) = 3
    AND count_if(event_type = 'Started' AND sequence_number = 0
      AND get_json_object(payload, '$.meterValue[0].sampledValue[0].value') = '100000') = 1
    AND count_if(event_type = 'Updated' AND sequence_number = 1
      AND get_json_object(payload, '$.meterValue[0].sampledValue[0].value') = '106000') = 1
    AND count_if(event_type = 'Ended' AND sequence_number = 2
      AND get_json_object(payload, '$.meterValue[0].sampledValue[0].value') = '112500') = 1,
  'Amount scenarios require the three intact smoke meter events.'
)
FROM IDENTIFIER(:raw_events_table_name)
WHERE run_id = 'smoke-run-v1';

SELECT assert_true(
  COUNT(*) = 1
    AND count_if(
      tariff_id = 'tariff-smoke-v1' AND currency = 'EUR'
        AND payload_hash = sha2(payload, 256)
        AND get_json_object(payload, '$.id') = tariff_id
        AND get_json_object(payload, '$.currency') = currency
        AND CAST(get_json_object(payload, '$.elements[0].price_components[0].price')
          AS DECIMAL(18,6)) = CAST(0.45 AS DECIMAL(18,6))
    ) = 1,
  'Amount scenarios require one intact EUR 0.45/kWh smoke tariff.'
)
FROM IDENTIFIER(:raw_tariffs_table_name)
WHERE run_id = 'smoke-run-v1';

SELECT assert_true(
  COUNT(*) = 2
    AND count_if(release_role = 'baseline') = 1
    AND count_if(release_role = 'candidate') = 1
    AND COUNT(DISTINCT payload_hash) = 1
    AND count_if(
      country_code = 'FR' AND party_id = 'CAS' AND cdr_id = 'cdr-smoke-v1'
        AND session_id = 'txn-smoke-v1' AND cdr_type = 'FINAL' AND currency = 'EUR'
        AND total_cost = CAST(5.63 AS DECIMAL(18,6))
        AND get_json_object(payload, '$.country_code') = country_code
        AND get_json_object(payload, '$.party_id') = party_id
        AND get_json_object(payload, '$.id') = cdr_id
        AND get_json_object(payload, '$.session_id') = session_id
        AND get_json_object(payload, '$.currency') = currency
        AND get_json_object(payload, '$.credit') = 'false'
        AND CAST(get_json_object(payload, '$.total_cost.excl_vat') AS DECIMAL(18,6)) = total_cost
        AND payload_hash = sha2(payload, 256)
        -- Exactly one literal is replaced, so retries cannot silently keep 5.63.
        AND length(payload) - length(replace(payload, '"excl_vat":5.63', ''))
          = length('"excl_vat":5.63')
    ) = 2,
  'Amount scenarios require two intact healthy smoke CDRs and one replaceable amount per body.'
)
FROM IDENTIFIER(:raw_cdrs_table_name)
WHERE run_id = 'smoke-run-v1';

-- Both manifests describe identical scenario inputs. Candidate identifiers are
-- synthetic fixture labels; they do not claim that real Git releases were run.
MERGE INTO IDENTIFIER(:run_manifest_table_name) AS target
USING (
  WITH fixture_runs AS (
    SELECT 'amount-bad-v1' AS run_id
    UNION ALL SELECT 'amount-fixed-v1' AS run_id
  )
  SELECT
    f.run_id,
    'amount-mismatch-v1' AS scenario_id,
    m.seed,
    m.baseline_sha,
    sha1(concat('candidate-', f.run_id)) AS candidate_sha,
    sha2(t.payload, 256) AS tariff_hash,
    m.created_at
  FROM fixture_runs AS f
  CROSS JOIN IDENTIFIER(:run_manifest_table_name) AS m
  CROSS JOIN IDENTIFIER(:raw_tariffs_table_name) AS t
  WHERE m.run_id = 'smoke-run-v1' AND t.run_id = 'smoke-run-v1'
) AS source
ON target.run_id = source.run_id
WHEN NOT MATCHED THEN INSERT (
  run_id, scenario_id, seed, baseline_sha, candidate_sha, tariff_hash, created_at
) VALUES (
  source.run_id, source.scenario_id, source.seed, source.baseline_sha,
  source.candidate_sha, source.tariff_hash, source.created_at
);

MERGE INTO IDENTIFIER(:raw_events_table_name) AS target
USING (
  WITH fixture_runs AS (
    SELECT 'amount-bad-v1' AS run_id
    UNION ALL SELECT 'amount-fixed-v1' AS run_id
  )
  SELECT
    f.run_id, e.event_id, e.charging_station_id, e.transaction_id,
    e.event_type, e.sequence_number, e.event_time, e.ingest_time,
    e.payload, e.payload_hash
  FROM fixture_runs AS f
  CROSS JOIN IDENTIFIER(:raw_events_table_name) AS e
  WHERE e.run_id = 'smoke-run-v1'
) AS source
ON target.run_id = source.run_id AND target.event_id = source.event_id
WHEN NOT MATCHED THEN INSERT (
  run_id, event_id, charging_station_id, transaction_id, event_type,
  sequence_number, event_time, ingest_time, payload, payload_hash
) VALUES (
  source.run_id, source.event_id, source.charging_station_id,
  source.transaction_id, source.event_type, source.sequence_number,
  source.event_time, source.ingest_time, source.payload, source.payload_hash
);

MERGE INTO IDENTIFIER(:raw_tariffs_table_name) AS target
USING (
  WITH fixture_runs AS (
    SELECT 'amount-bad-v1' AS run_id
    UNION ALL SELECT 'amount-fixed-v1' AS run_id
  )
  SELECT
    f.run_id, t.tariff_id, t.valid_from, t.valid_to,
    t.currency, t.payload, t.payload_hash
  FROM fixture_runs AS f
  CROSS JOIN IDENTIFIER(:raw_tariffs_table_name) AS t
  WHERE t.run_id = 'smoke-run-v1'
) AS source
ON target.run_id = source.run_id AND target.tariff_id = source.tariff_id
WHEN NOT MATCHED THEN INSERT (
  run_id, tariff_id, valid_from, valid_to, currency, payload, payload_hash
) VALUES (
  source.run_id, source.tariff_id, source.valid_from, source.valid_to,
  source.currency, source.payload, source.payload_hash
);

-- Preserve all CDR bytes except the deliberate bad candidate amount, then
-- derive its amount projection and integrity hash from the resulting payload.
MERGE INTO IDENTIFIER(:raw_cdrs_table_name) AS target
USING (
  WITH fixture_runs AS (
    SELECT 'amount-bad-v1' AS run_id
    UNION ALL SELECT 'amount-fixed-v1' AS run_id
  ), fixture_responses AS (
    SELECT
      f.run_id, c.release_role, c.country_code, c.party_id, c.cdr_id,
      c.session_id, c.cdr_type, c.currency, c.ingest_time,
      CASE
        WHEN f.run_id = 'amount-bad-v1' AND c.release_role = 'candidate'
          THEN replace(c.payload, '"excl_vat":5.63', '"excl_vat":6.50')
        ELSE c.payload
      END AS payload
    FROM fixture_runs AS f
    CROSS JOIN IDENTIFIER(:raw_cdrs_table_name) AS c
    WHERE c.run_id = 'smoke-run-v1'
  )
  SELECT
    run_id, release_role, country_code, party_id, cdr_id, session_id,
    cdr_type, currency,
    CAST(get_json_object(payload, '$.total_cost.excl_vat') AS DECIMAL(18,6)) AS total_cost,
    payload, sha2(payload, 256) AS payload_hash, ingest_time
  FROM fixture_responses
) AS source
ON target.run_id = source.run_id
AND target.release_role = source.release_role
AND target.payload_hash = source.payload_hash
WHEN NOT MATCHED THEN INSERT (
  run_id, release_role, country_code, party_id, cdr_id, session_id,
  cdr_type, currency, total_cost, payload, payload_hash, ingest_time
) VALUES (
  source.run_id, source.release_role, source.country_code, source.party_id,
  source.cdr_id, source.session_id, source.cdr_type, source.currency,
  source.total_cost, source.payload, source.payload_hash, source.ingest_time
);

-- INSERT-only loading must never silently accept conflicting existing evidence.
-- Check each run separately, including missing rows via the explicit run grid.
WITH fixture_runs AS (
  SELECT 'amount-bad-v1' AS run_id
  UNION ALL SELECT 'amount-fixed-v1' AS run_id
)
SELECT f.run_id, assert_true(
  COUNT(t.run_id) = 1
    AND count_if(
      t.scenario_id = 'amount-mismatch-v1' AND t.seed = s.seed
        AND t.baseline_sha = s.baseline_sha
        AND t.candidate_sha = sha1(concat('candidate-', f.run_id))
        AND t.tariff_hash = sha2(p.payload, 256) AND t.created_at = s.created_at
    ) = 1,
  'Amount scenario manifest is missing, duplicated or conflicts with its immutable inputs.'
)
FROM fixture_runs AS f
LEFT JOIN IDENTIFIER(:run_manifest_table_name) AS t ON t.run_id = f.run_id
CROSS JOIN IDENTIFIER(:run_manifest_table_name) AS s
CROSS JOIN IDENTIFIER(:raw_tariffs_table_name) AS p
WHERE s.run_id = 'smoke-run-v1' AND p.run_id = 'smoke-run-v1'
GROUP BY f.run_id;

WITH fixture_runs AS (
  SELECT 'amount-bad-v1' AS run_id
  UNION ALL SELECT 'amount-fixed-v1' AS run_id
)
SELECT f.run_id, assert_true(
  COUNT(t.run_id) = 3 AND COUNT(DISTINCT t.event_id) = 3
    AND count_if(
      t.charging_station_id = s.charging_station_id
        AND t.transaction_id = s.transaction_id AND t.event_type = s.event_type
        AND t.sequence_number = s.sequence_number AND t.event_time = s.event_time
        AND t.ingest_time = s.ingest_time AND t.payload = s.payload
        AND t.payload_hash = s.payload_hash AND t.payload_hash = sha2(t.payload, 256)
    ) = 3,
  'Amount scenario events must be exactly three unchanged smoke events with valid hashes.'
)
FROM fixture_runs AS f
LEFT JOIN IDENTIFIER(:raw_events_table_name) AS t ON t.run_id = f.run_id
LEFT JOIN IDENTIFIER(:raw_events_table_name) AS s
  ON s.run_id = 'smoke-run-v1' AND s.event_id = t.event_id
GROUP BY f.run_id;

WITH fixture_runs AS (
  SELECT 'amount-bad-v1' AS run_id
  UNION ALL SELECT 'amount-fixed-v1' AS run_id
)
SELECT f.run_id, assert_true(
  COUNT(t.run_id) = 1
    AND count_if(
      t.valid_from = s.valid_from
        AND (t.valid_to = s.valid_to OR (t.valid_to IS NULL AND s.valid_to IS NULL))
        AND t.currency = s.currency AND t.payload = s.payload
        AND t.payload_hash = s.payload_hash AND t.payload_hash = sha2(t.payload, 256)
    ) = 1,
  'Amount scenario tariff must match the unchanged smoke tariff and its hash.'
)
FROM fixture_runs AS f
LEFT JOIN IDENTIFIER(:raw_tariffs_table_name) AS t ON t.run_id = f.run_id
LEFT JOIN IDENTIFIER(:raw_tariffs_table_name) AS s
  ON s.run_id = 'smoke-run-v1' AND s.tariff_id = t.tariff_id
GROUP BY f.run_id;

WITH fixture_runs AS (
  SELECT 'amount-bad-v1' AS run_id
  UNION ALL SELECT 'amount-fixed-v1' AS run_id
)
SELECT f.run_id, assert_true(
  COUNT(t.run_id) = 2
    AND count_if(t.release_role = 'baseline') = 1
    AND count_if(t.release_role = 'candidate') = 1
    AND count_if(
      t.country_code = s.country_code AND t.party_id = s.party_id
        AND t.cdr_id = s.cdr_id AND t.session_id = s.session_id
        AND t.cdr_type = s.cdr_type AND t.currency = s.currency
        AND t.ingest_time = s.ingest_time
        AND t.payload = CASE
          WHEN f.run_id = 'amount-bad-v1' AND t.release_role = 'candidate'
            THEN replace(s.payload, '"excl_vat":5.63', '"excl_vat":6.50')
          ELSE s.payload
        END
        AND t.total_cost = CAST(get_json_object(t.payload, '$.total_cost.excl_vat') AS DECIMAL(18,6))
        AND t.total_cost = CASE
          WHEN f.run_id = 'amount-bad-v1' AND t.release_role = 'candidate'
            THEN CAST(6.50 AS DECIMAL(18,6))
          ELSE CAST(5.63 AS DECIMAL(18,6))
        END
        AND t.payload_hash = sha2(t.payload, 256)
    ) = 2,
  'Amount scenario CDRs must contain one response per release; only the bad candidate reports EUR 6.50.'
)
FROM fixture_runs AS f
LEFT JOIN IDENTIFIER(:raw_cdrs_table_name) AS t ON t.run_id = f.run_id
LEFT JOIN IDENTIFIER(:raw_cdrs_table_name) AS s
  ON s.run_id = 'smoke-run-v1' AND s.release_role = t.release_role
GROUP BY f.run_id;

SELECT run_id, release_role, cdr_id, total_cost, payload_hash
FROM IDENTIFIER(:raw_cdrs_table_name)
WHERE run_id IN ('amount-bad-v1', 'amount-fixed-v1')
ORDER BY run_id, release_role;
