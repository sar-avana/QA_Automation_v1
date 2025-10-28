WITH rx_dupes AS (
  SELECT claim_id, patient_token_1, patient_token_2, COUNT(*) AS dup_count
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.PHARMACY_{{REVIEW_SUFFIX}}')
  GROUP BY claim_id, patient_token_1, patient_token_2
  HAVING COUNT(*) > 1
),
rx_total AS (
  SELECT COUNT(*) AS total_rows
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.PHARMACY_{{REVIEW_SUFFIX}}')
),
rx_dupe_total AS (
  SELECT COALESCE(SUM(dup_count), 0) AS duplicate_rows
  FROM rx_dupes
),
mh_dupes AS (
  SELECT encounter_key, patient_token_1, patient_token_2, COUNT(*) AS dup_count
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.MEDICAL_HEADERS_{{REVIEW_SUFFIX}}')
  GROUP BY encounter_key, patient_token_1, patient_token_2
  HAVING COUNT(*) > 1
),
mh_total AS (
  SELECT COUNT(*) AS total_rows
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.MEDICAL_HEADERS_{{REVIEW_SUFFIX}}')
),
mh_dupe_total AS (
  SELECT COALESCE(SUM(dup_count), 0) AS duplicate_rows
  FROM mh_dupes
)
SELECT
  rx_dupe_total.duplicate_rows AS pharmacy_duplicate_rows,
  rx_total.total_rows          AS pharmacy_total_rows,
  ROUND(rx_dupe_total.duplicate_rows / NULLIF(rx_total.total_rows, 0) * 100, 2) AS pharmacy_duplicate_percentage,
  mh_dupe_total.duplicate_rows AS headers_duplicate_rows,
  mh_total.total_rows          AS headers_total_rows,
  ROUND(mh_dupe_total.duplicate_rows / NULLIF(mh_total.total_rows, 0) * 100, 2) AS headers_duplicate_percentage
FROM rx_dupe_total, rx_total, mh_dupe_total, mh_total;