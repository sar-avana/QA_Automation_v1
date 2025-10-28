
WITH pharmacy_dupes AS (
    SELECT
        pharmacy_event_id,
        patient_id,
        COUNT(*) AS dup_count
    FROM {{DB}}.{{SCHEMA}}.pharmacy_events_REVIEW_LATEST
    GROUP BY pharmacy_event_id, patient_id
    HAVING COUNT(*) > 1
),
pharmacy_total AS (
    SELECT COUNT(*) AS total_rows
    FROM {{DB}}.{{SCHEMA}}.pharmacy_events_REVIEW_LATEST
),
pharmacy_dupe_total AS (
    SELECT SUM(dup_count) AS duplicate_rows
    FROM pharmacy_dupes
),
medical_dupes AS (
    SELECT
        medical_event_id,
        patient_id,
        COUNT(*) AS dup_count
    FROM {{DB}}.{{SCHEMA}}.medical_events_REVIEW_LATEST
    GROUP BY medical_event_id, patient_id
    HAVING COUNT(*) > 1
),
medical_total AS (
    SELECT COUNT(*) AS total_rows
    FROM {{DB}}.{{SCHEMA}}.medical_events_REVIEW_LATEST
),
medical_dupe_total AS (
    SELECT SUM(dup_count) AS duplicate_rows
    FROM medical_dupes
)
SELECT
    pharmacy_dupe_total.duplicate_rows AS pharmacy_duplicate_rows,
    pharmacy_total.total_rows AS pharmacy_total_rows,
    ROUND((pharmacy_dupe_total.duplicate_rows / pharmacy_total.total_rows) * 100, 2) AS pharmacy_duplicate_percentage,
    medical_dupe_total.duplicate_rows AS medical_duplicate_rows,
    medical_total.total_rows AS medical_total_rows,
    ROUND((medical_dupe_total.duplicate_rows / medical_total.total_rows) * 100, 2) AS medical_duplicate_percentage
FROM pharmacy_dupe_total, pharmacy_total, medical_dupe_total, medical_total;