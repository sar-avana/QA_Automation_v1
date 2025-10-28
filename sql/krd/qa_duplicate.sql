WITH rx_dupes AS (
  SELECT pharmacy_event_id, patient_id, COUNT(*) AS dup_count
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.PHARMACY_EVENTS_{{REVIEW_SUFFIX}}')
  GROUP BY pharmacy_event_id, patient_id
  HAVING COUNT(*) > 1
),
rx_total AS (
  SELECT COUNT(*) AS total_rows
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.PHARMACY_EVENTS_{{REVIEW_SUFFIX}}')
),
rx_dupe_total AS (
  SELECT COALESCE(SUM(dup_count), 0) AS duplicate_rows
  FROM rx_dupes
),
ip_dupes AS (
  SELECT utilization_id, patient_id, COUNT(*) AS dup_count
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.INPATIENT_EVENTS_{{REVIEW_SUFFIX}}')
  GROUP BY utilization_id, patient_id
  HAVING COUNT(*) > 1
),
ip_total AS (
  SELECT COUNT(*) AS total_rows
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.INPATIENT_EVENTS_{{REVIEW_SUFFIX}}')
),
ip_dupe_total AS (
  SELECT COALESCE(SUM(dup_count), 0) AS duplicate_rows
  FROM ip_dupes
),
nip_dupes AS (
  SELECT utilization_id, patient_id, COUNT(*) AS dup_count
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.NON_INPATIENT_EVENTS_{{REVIEW_SUFFIX}}')
  GROUP BY utilization_id, patient_id
  HAVING COUNT(*) > 1
),
nip_total AS (
  SELECT COUNT(*) AS total_rows
  FROM IDENTIFIER('{{DB}}.{{SCHEMA}}.NON_INPATIENT_EVENTS_{{REVIEW_SUFFIX}}')
),
nip_dupe_total AS (
  SELECT COALESCE(SUM(dup_count), 0) AS duplicate_rows
  FROM nip_dupes
)
SELECT
  rx_dupe_total.duplicate_rows     AS rx_duplicate_rows,
  rx_total.total_rows              AS rx_total_rows,
  ROUND(rx_dupe_total.duplicate_rows / NULLIF(rx_total.total_rows, 0) * 100, 2) AS rx_duplicate_percentage,
  ip_dupe_total.duplicate_rows     AS inpatient_duplicate_rows,
  ip_total.total_rows              AS inpatient_total_rows,
  ROUND(ip_dupe_total.duplicate_rows / NULLIF(ip_total.total_rows, 0) * 100, 2) AS inpatient_duplicate_percentage,
  nip_dupe_total.duplicate_rows    AS non_inpatient_duplicate_rows,
  nip_total.total_rows             AS non_inpatient_total_rows,
  ROUND(nip_dupe_total.duplicate_rows / NULLIF(nip_total.total_rows, 0) * 100, 2) AS non_inpatient_duplicate_percentage
FROM rx_dupe_total, rx_total, ip_dupe_total, ip_total, nip_dupe_total, nip_total;