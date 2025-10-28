import pandas as pd
import snowflake.connector
from datetime import datetime
import yaml
from tabulate import tabulate
from fpdf import FPDF
import os
import smtplib
from email.message import EmailMessage
import json
import ast
import re

# -------------------------
# Helper: email sender
# -------------------------
def send_email(to, subject, body, attachments=None):
    if not to:
        return

    msg = EmailMessage()
    msg["To"] = to
    msg["From"] = EMAIL_USER
    msg["Subject"] = subject
    msg.set_content(body)

    if attachments:
        for file in attachments:
            if os.path.exists(file):
                with open(file, "rb") as f:
                    msg.add_attachment(
                        f.read(),
                        maintype="application",
                        subtype="octet-stream",
                        filename=os.path.basename(file)
                    )

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"✉️ Email sent to {to}")
    except Exception as e:
        print(f"⚠️ Failed to send email to {to}: {e}")

# -------------------------
# Config / constants
# -------------------------
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "sivasaravanan9578@gmail.com"
EMAIL_PASSWORD = "mdey njcz uwky tfgz"

# Base SQL templates executed once per DB.Schema (file basenames only)
sql_files = ["qa_date_range.sql", "qa_duplicate.sql", "qa_rowcount.sql"]

# Data structures
qa_summary_list = []
qa_detailed_grouped = {}
DELTA_THRESHOLD = 2

# Fabric aliases (normalized folder names under sql/fabric/<normalized>/)
FABRIC_FOLDER_ALIASES = {
    "plaid": "plaid",
    "krd": "krd",
    "houndstooth": "ht",
    "ht": "ht",
    "encounters": "ht",
    "encounters+": "ht"
}

def _normalize_fabric_name(fabric_val):
    """
    Normalize the fabric string to a folder name under sql/fabric.
    Returns None if fabric_val is empty/None.
    """
    if not fabric_val or not str(fabric_val).strip():
        return None
    f = str(fabric_val).strip().lower()
    return FABRIC_FOLDER_ALIASES.get(f, f)  # default to given string if unknown

def _resolve_sql_paths_for_fabric(fabric_norm, base_files):
    """
    Given a normalized fabric name (e.g., 'plaid', 'krd', 'ht') and the base file names,
    return a list of file paths to execute for this run.
    Priority:
      1) sql/fabric/<fabric_norm>/<file>
      2) <file>  (root fallback)
    """
    effective_paths = []
    for base in base_files:
        # Preferred: fabric-specific path
        fabric_path = os.path.join("sql", "fabric", fabric_norm, base) if fabric_norm else None
        if fabric_path and os.path.exists(fabric_path):
            effective_paths.append(fabric_path)
        else:
            # Fallback to original root-level file
            if fabric_path and not os.path.exists(fabric_path):
                print(f"ℹ️ Fabric-specific SQL not found ({fabric_path}); falling back to {base}")
            effective_paths.append(base)
    return effective_paths

# -------------------------
# Load config & connect
# -------------------------
config_df = pd.read_csv("db_schema_config.csv")
if "Status" not in config_df.columns:
    config_df["Status"] = ""
if "Fabric" not in config_df.columns:
    # Optional column; leaving blank preserves legacy behavior
    config_df["Fabric"] = ""

with open("sf_config.yaml", "r") as f:
    sf_conf = yaml.safe_load(f)['snowflake']

conn = snowflake.connector.connect(
    account=sf_conf['account'],
    user=sf_conf['user'],
    warehouse=sf_conf['warehouse'],
    role=sf_conf['role'],
    authenticator=sf_conf['authenticator']
)
cur = conn.cursor()
print("✅ SSO Login Successful!")

# -------------------------
# Utility: safe parse of table_info (kept for later use)
# -------------------------
def parse_table_info(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        try:
            return ast.literal_eval(raw)
        except Exception:
            print("⚠️ Failed to parse table_info JSON:", raw)
            return {}

# -------------------------
# New helpers: extract suffixes & prepare SQL
# -------------------------
def _extract_suffix_from_table_str(table_str):
    """
    Given a table string like 'MEDICAL_EVENTS_20241020' or similar,
    return the suffix part (digits after last underscore). Returns None if not found.
    """
    if not table_str or not isinstance(table_str, str):
        return None
    # match trailing underscore + digits (6-10 digits typical YYYYMMDD or YYYYMM)
    m = re.search(r'_([0-9]{6,10})$', table_str)
    if m:
        return m.group(1)
    # fallback: if last token after '_' is alphanumeric and not empty, return it
    parts = table_str.rsplit('_', 1)
    if len(parts) == 2 and parts[1]:
        return parts[1]
    return None

def _extract_suffix_from_table_info(table_info):
    """
    table_info may be a dict (key -> table_name), a list of table names, or a string.
    Returns the first suffix found among values, or None.
    """
    if not table_info:
        print("DEBUG: No table_info provided for suffix extraction")
        return None

    if isinstance(table_info, dict):
        iterable = table_info.values()
    elif isinstance(table_info, (list, tuple, set)):
        iterable = table_info
    else:
        iterable = [table_info]

    for val in iterable:
        if isinstance(val, dict):
            for inner in val.values():
                suffix = _extract_suffix_from_table_str(str(inner))
                if suffix:
                    print(f"DEBUG: Extracted suffix '{suffix}' from value '{inner}'")
                    return suffix
            continue

        suffix = _extract_suffix_from_table_str(str(val))
        if suffix:
            print(f"DEBUG: Extracted suffix '{suffix}' from value '{val}'")
            return suffix

    print("DEBUG: No valid suffix extracted from table_info")
    return None

def get_suffixes_from_metadata_rows(metadata_rows):
    """
    metadata_rows: list of tuples (cohort_id, status, table_info)
    Returns (review_suffix, latest_suffix)
    Logic:
        - review_suffix: first row with status == 'QA'
        - latest_suffix: prefer row with status == 'LATEST'; else immediate next row after the QA row; else first non-QA row
    """
    if not metadata_rows:
        raise ValueError("Empty metadata_rows")

    # Normalize rows into list of dicts for convenience
    normalized = []
    for r in metadata_rows:
        cohort_id = r[0] if len(r) > 0 else None
        status = r[1] if len(r) > 1 else None
        table_info_raw = r[2] if len(r) > 2 else None
        table_info = parse_table_info(table_info_raw) if table_info_raw else {}
        normalized.append({
            "cohort_id": cohort_id,
            "status": str(status).strip() if status is not None else "",
            "table_info_raw": table_info_raw,
            "table_info": table_info
        })

    # Find review row (first with status == "QA")
    review_row_idx = None
    for i, row in enumerate(normalized):
        if row["status"] == "QA":
            review_row_idx = i
            break

    if review_row_idx is None:
        raise ValueError("No metadata row with status 'QA' found")

    # extract review suffix
    review_row = normalized[review_row_idx]
    review_suffix = _extract_suffix_from_table_info(review_row["table_info"])
    if not review_suffix:
        review_suffix = _extract_suffix_from_table_info(review_row["table_info_raw"])

    # Find latest suffix candidate:
    latest_suffix = None
    # Prefer a row explicitly marked 'LATEST'
    for i, row in enumerate(normalized):
        if row["status"] == "LATEST":
            latest_suffix = _extract_suffix_from_table_info(row["table_info"])
            if not latest_suffix:
                latest_suffix = _extract_suffix_from_table_info(row["table_info_raw"])
            if latest_suffix:
                break

    # If no explicit LATEST row, pick the first row after the QA row (older run)
    if not latest_suffix:
        if review_row_idx + 1 < len(normalized):
            candidate = normalized[review_row_idx + 1]
            latest_suffix = _extract_suffix_from_table_info(candidate["table_info"])
            if not latest_suffix:
                latest_suffix = _extract_suffix_from_table_info(candidate["table_info_raw"])

    # As a last fallback, scan all rows for the first non-QA row and use it
    if not latest_suffix:
        for i, row in enumerate(normalized):
            if row["status"] != "QA":
                latest_suffix = _extract_suffix_from_table_info(row["table_info"])
                if not latest_suffix:
                    latest_suffix = _extract_suffix_from_table_info(row["table_info_raw"])
                if latest_suffix:
                    break

    if not review_suffix or not latest_suffix:
        raise ValueError(f"Could not resolve suffixes from metadata (review: {review_suffix}, latest: {latest_suffix})")

    return review_suffix, latest_suffix

def prepare_sql(sql_template, db, schema, review_suffix, latest_suffix):
    """
    Replace main placeholders in SQL template.
    """
    return (sql_template
            .replace("{{DB}}", db)
            .replace("{{SCHEMA}}", schema)
            .replace("{{REVIEW_SUFFIX}}", review_suffix)
            .replace("{{LATEST_SUFFIX}}", latest_suffix))

# -------------------------
# Main loop: one QA per DB.Schema
# -------------------------
for idx, row in config_df.iterrows():
    db = row['Database']
    schema = row['Schema']
    status_flag = row.get('Status', "")
    owner_email = str(row.get("Email", "") or "").strip()
    fabric_val = row.get("Fabric", "")
    fabric_norm = _normalize_fabric_name(fabric_val)

    key = f"{db}_{schema}"
    print(f"\n📌 Processing: {db}.{schema} (Fabric: {fabric_val or 'N/A'})")

    # Skip already-done schemas
    if str(status_flag).strip().lower() == "qa done":
        print(f"⏩ Skipping {db}.{schema}, QA already completed earlier")
        continue

    # Read cohort metadata to decide whether to run QA
    try:
        cur.execute(f"""
            SELECT cohort_id, status, table_info
            FROM {db}.internal.cohort_metadata
            ORDER BY run_start_time DESC
        """)
        metadata_rows = cur.fetchall()
    except Exception as e:
        print(f"⚠️ Failed to query cohort_metadata for {db}.internal: {e}")
        qa_summary_list.append({
            "database": db,
            "schema": schema,
            "review_table": "",
            "latest_table": "",
            "review_row_count": 0,
            "latest_row_count": 0,
            "status": "ERROR",
            "checked_at": datetime.now()
        })
        config_df.at[idx, "Status"] = "ERROR"
        continue

    if not metadata_rows:
        print(f"⚠️ No cohort metadata found for {db}.{schema}")
        qa_summary_list.append({
            "database": db,
            "schema": schema,
            "review_table": "",
            "latest_table": "",
            "review_row_count": 0,
            "latest_row_count": 0,
            "status": "NO_METADATA",
            "checked_at": datetime.now()
        })
        config_df.at[idx, "Status"] = "NO_METADATA"
        continue

    # Latest row decides QA
    latest_row = metadata_rows[0]
    latest_cohort_id, latest_status, latest_tableinfo_raw = latest_row

    prev_row = metadata_rows[1] if len(metadata_rows) > 1 else None
    prev_cohort_id = None
    prev_tableinfo_raw = None
    if prev_row:
        prev_cohort_id, _, prev_tableinfo_raw = prev_row

    # Parse table_info but do not iterate tables (kept for CSV & future use)
    latest_tables = parse_table_info(latest_tableinfo_raw)
    prev_tables = parse_table_info(prev_tableinfo_raw) if prev_tableinfo_raw else {}

    # If latest status is not QA, skip
    if str(latest_status).strip() != "QA":
        print(f"✅ Latest run for {db}.{schema} has status '{latest_status}' → No QA required")
        qa_summary_list.append({
            "database": db,
            "schema": schema,
            "review_table": f"cohort:{latest_cohort_id}",
            "latest_table": f"cohort:{prev_cohort_id}" if prev_cohort_id else "",
            "review_row_count": 0,
            "latest_row_count": 0,
            "status": "SKIPPED_RELEASED",
            "checked_at": datetime.now()
        })
        config_df.at[idx, "Status"] = "Released"
        continue

    # At this point, run QA once per DB.Schema
    print(f"🔍 Running QA once for schema → {db}.{schema} (cohort:{latest_cohort_id})")

    # Prepare qa_detailed_grouped entry
    if key not in qa_detailed_grouped:
        qa_detailed_grouped[key] = {
            "details": [],
            "db": db,
            "schema": schema,
            "owner_email": owner_email
        }

    # Create summary strings (preserve previous CSV layout)
    review_table_str = ",".join(latest_tables.values()) if isinstance(latest_tables, dict) else (",".join(latest_tables) if latest_tables else "")
    latest_table_str = ",".join(prev_tables.values()) if isinstance(prev_tables, dict) else (",".join(prev_tables) if prev_tables else "")

    # Derive suffixes from metadata_rows
    try:
        review_suffix, latest_suffix = get_suffixes_from_metadata_rows(metadata_rows)
        print(f"   → Using REVIEW_SUFFIX={review_suffix} and LATEST_SUFFIX={latest_suffix}")
    except Exception as e:
        print(f"⚠️ Failed to derive table suffixes from metadata for {db}.{schema}: {e}")
        qa_detailed_grouped[key]["details"].append({
            "database": db,
            "schema": schema,
            "qa_table": "",
            "prev_table": "",
            "sql_file": "N/A",
            "error": f"Failed to derive suffixes: {e}",
            "checked_at": datetime.now()
        })
        qa_summary_list.append({
            "database": db,
            "schema": schema,
            "review_table": review_table_str,
            "latest_table": latest_table_str,
            "review_row_count": 0,
            "latest_row_count": 0,
            "status": "ERROR_SUFFIX",
            "checked_at": datetime.now()
        })
        config_df.at[idx, "Status"] = "ERROR"
        continue

    # Resolve SQL files for this fabric (with fallback to root)
    effective_sql_paths = _resolve_sql_paths_for_fabric(fabric_norm, sql_files)

    # Execute each SQL file exactly once per schema.
    # SQL files are expected to use {{DB}}, {{SCHEMA}}, {{REVIEW_SUFFIX}} and {{LATEST_SUFFIX}} placeholders.
    for sql_path in effective_sql_paths:
        try:
            with open(sql_path, 'r') as f:
                raw_sql = f.read()
        except Exception as e:
            print(f"⚠️ Failed to read {sql_path}: {e}")
            qa_detailed_grouped[key]["details"].append({
                "database": db,
                "schema": schema,
                "qa_table": "",
                "prev_table": "",
                "sql_file": sql_path,
                "error": f"Failed to read SQL file: {e}",
                "checked_at": datetime.now()
            })
            continue

        # First replace the DB/SCHEMA and the two suffix placeholders
        try:
            sql_with_suffixes = prepare_sql(raw_sql, db, schema, review_suffix, latest_suffix)
        except Exception as e:
            print(f"⚠️ Failed to prepare SQL for {sql_path}: {e}")
            qa_detailed_grouped[key]["details"].append({
                "database": db,
                "schema": schema,
                "qa_table": "",
                "prev_table": "",
                "sql_file": sql_path,
                "error": f"Failed to prepare SQL: {e}",
                "checked_at": datetime.now()
            })
            continue

        # Maintain older placeholder replacements as safety (kept for backwards compatibility)
        sql_query = (sql_with_suffixes
                     .replace("{{TABLE}}", "")
                     .replace("{{REVIEW_TABLE}}", "")
                     .replace("{{PREV_TABLE}}", "")
                     .replace("{{PREVIOUS_TABLE}}", "")
                     )

        try:
            cur.execute(sql_query)
            col_names = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()

            for r in rows:
                row_dict = dict(zip(col_names, r)) if col_names else {}
                entry = {
                    **row_dict,
                    "database": db,
                    "schema": schema,
                    "tablename": f"{db}.{schema}",   # schema-level provenance
                    "qa_table": review_table_str,
                    "prev_table": latest_table_str,
                    "sql_file": sql_path,
                    "checked_at": datetime.now()
                }
                qa_detailed_grouped[key]["details"].append(entry)

        except Exception as e:
            print(f"⚠️ SQL Error in {sql_path} for schema {db}.{schema}: {e}")
            qa_detailed_grouped[key]["details"].append({
                "database": db,
                "schema": schema,
                "tablename": f"{db}.{schema}",
                "qa_table": review_table_str,
                "prev_table": latest_table_str,
                "sql_file": sql_path,
                "error": str(e),
                "checked_at": datetime.now()
            })

    # After QA run: append summary & update config status
    qa_summary_list.append({
        "database": db,
        "schema": schema,
        "review_table": review_table_str,
        "latest_table": latest_table_str,
        "review_row_count": 0,
        "latest_row_count": 0,
        "status": "QA_EXECUTED",
        "checked_at": datetime.now()
    })

    config_df.at[idx, "Status"] = "QA done"
    print(f"✅ Status updated → QA done for {db}.{schema}")

# -------------------------
# Persist summary & detailed outputs
# -------------------------
config_df.to_csv("db_schema_config.csv", index=False)
print("\n📌 db_schema_config.csv updated with status")

qa_summary_df = pd.DataFrame(qa_summary_list)
qa_summary_df.to_csv("qa_summary.csv", index=False)
print("✅ qa_summary.csv generated")

# -------------------------
# Process metrics and alerts and send email per DB.Schema
# -------------------------
for key, group in qa_detailed_grouped.items():
    detail_df = pd.DataFrame(group["details"])
    detail_df.to_csv(f"{key}.csv", index=False)
    print(f"📌 Detailed QA saved → {key}.csv")

    alerts = []

    # Helper: resolve the most specific table name present in the row
    def _resolve_result_table_name(row, default_key):
        # Prefer explicit column from SQL output if present
        cand = row.get('TABLE_NAME') or row.get('table_name') or row.get('TABLE') or None
        if cand and isinstance(cand, str) and cand.strip():
            return cand
        # Fallback to what we stored as provenance
        cand = row.get('tablename')
        if cand and isinstance(cand, str) and cand.strip():
            return cand
        return default_key

    # Helper: friendly table label (optional for readability)
    def _friendly_table_label(name):
        try:
            return name.replace('_', ' ').strip()
        except Exception:
            return str(name)

    # Stale date detection across categories
    stale_buckets = {
        "INPATIENT DATE": {"latest": None, "previous": None},
        "NON_INPATIENT SERVICE DATE": {"latest": None, "previous": None},
        "SERVICE LINE DATE": {"latest": None, "previous": None},
        "SERVICE DATE": {"latest": None, "previous": None},
        "RX FILL DATE": {"latest": None, "previous": None},
    }

    def _assign_stale_value(test_label, result_val):
        if not isinstance(test_label, str):
            return
        t = test_label.upper()
        try:
            d = pd.to_datetime(result_val, errors='coerce')
            d = d.date() if pd.notnull(d) else None
        except Exception:
            d = None
        if d is None:
            return

        is_latest = "LATEST" in t
        is_previous = ("PREVIOUS" in t) or ("(PREVIOUS)" in t)

        if "INPATIENT" in t and "DATE" in t:
            bucket = "INPATIENT DATE"
        elif "NON_INPATIENT" in t and "SERVICE_DATE" in t:
            bucket = "NON_INPATIENT SERVICE DATE"
        elif ("SERVICE_LINE" in t or "SERVICE LINE" in t) and "DATE" in t:
            bucket = "SERVICE LINE DATE"
        elif "FILL_DATE" in t:
            bucket = "RX FILL DATE"
        elif "SERVICE_DATE" in t:
            bucket = "SERVICE DATE"
        else:
            return

        if is_latest:
            stale_buckets[bucket]["latest"] = d
        elif is_previous:
            stale_buckets[bucket]["previous"] = d

    # Collect stale-date inputs
    for _, r in detail_df.iterrows():
        test = r.get('TEST')
        result = r.get('RESULT')
        _assign_stale_value(test, result)

    # Evaluate stale per bucket
    for bucket, vals in stale_buckets.items():
        latest = vals["latest"]
        prev = vals["previous"]
        if latest and prev and latest == prev:
            alerts.append({
                "alert_type": f"{bucket} STALE",
                "message": f"{bucket} SAME → {latest}",
                "table_name": bucket,
                "timestamp": datetime.now()
            })

        # Helper: resolve specific table name for dupe columns (works across KRD/HT/PLAID)
    def _map_dupe_col_to_table(col_name):
        cu = str(col_name).upper()
        # KRD
        if "NON_INPATIENT" in cu:
            return "NON_INPATIENT_EVENTS"
        if "INPATIENT" in cu:
            return "INPATIENT_EVENTS"
        if "RX" in cu or "PHARMACY" in cu:
            # Prefer KRD name; HT still reads well
            return "PHARMACY_EVENTS"
        # Encounters/HT
        if "HEADERS" in cu:
            return "MEDICAL_HEADERS"
        if "SERVICE_LINES" in cu or "SERVICE_LINE" in cu:
            return "MEDICAL_SERVICE_LINES"
        # PLAID/generic
        if "MEDICAL" in cu:
            return "MEDICAL_EVENTS"
        # Fallback
        return None

    # Duplicate percentage detection (dynamic)
    dup_pct_cols = [c for c in detail_df.columns
                    if isinstance(c, str) and c.upper().endswith("DUPLICATE_PERCENTAGE")]

    for col in dup_pct_cols:
        detail_df[col] = pd.to_numeric(detail_df[col], errors='coerce')
        dup_rows = detail_df[detail_df[col] > 0]
        for _, r in dup_rows.iterrows():
            pct = r[col]

            # Prefer mapping from column name; fallback to any per-row table name; else schema provenance
            mapped_tbl = _map_dupe_col_to_table(col)
            if mapped_tbl:
                table_for_row = mapped_tbl
            else:
                table_for_row = _resolve_result_table_name(r, key)

            alerts.append({
                "alert_type": f"{table_for_row} DUPLICATES",
                "message": f"Duplicates detected -> {pct:.2f}%",  # ASCII arrow to avoid Excel mojibake
                "table_name": table_for_row,
                "timestamp": datetime.now()
            })

    # Row/Patient delta checks (now labeled with the specific table)
    delta_cols = [("ROW_DELTA_PCT", "ROW DELTA"), ("PATIENT_DELTA_PCT", "PATIENT DELTA")]
    for col, base_alert_type in delta_cols:
        if col in detail_df.columns:
            detail_df[col] = pd.to_numeric(detail_df[col], errors='coerce')
            delta = detail_df[detail_df[col].abs() > DELTA_THRESHOLD]
            for _, r in delta.iterrows():
                table_for_row = _resolve_result_table_name(r, key)
                # Include table in the alert_type, e.g., "INPATIENT_EVENTS ROW DELTA"
                alerts.append({
                    "alert_type": f"{table_for_row} {base_alert_type}",
                    "message": f"{_friendly_table_label(table_for_row)} {base_alert_type.lower()} > ±{DELTA_THRESHOLD}% → {r[col]:.2f}%",
                    "table_name": table_for_row,
                    "timestamp": datetime.now()
                })

    # Write metric alerts
    attachments = [f"{key}.csv"]
    if alerts:
        metric_file = f"metric_{key}.csv"
        pd.DataFrame(alerts).to_csv(metric_file, index=False)
        attachments.append(metric_file)
        print(f"🚨 Metric alerts generated → {metric_file}")

    # SEND EMAIL to correct schema owner
    owner_email = group.get("owner_email")
    db = group.get("db")
    schema = group.get("schema")
    if owner_email:
        subject = f"QA Report & Alerts for {db}.{schema}"
        body = f"Hi,\n\nPlease find attached the QA summary and any metric alerts for {db}.{schema}.\n\nRegards,\nQA Automation"
        send_email(owner_email, subject, body, attachments=attachments)

# -------------------------
# Final PDF summary
# -------------------------
pdf = FPDF()
pdf.add_page()
pdf.set_font("Courier", size=10)
pdf.cell(200, 10, "QA Summary Report", ln=True, align='C')
pdf.ln(5)

for line in tabulate(qa_summary_df, headers='keys', tablefmt='grid').split('\n'):
    pdf.multi_cell(0, 5, line)

pdf.output("qa_report.pdf")

cur.close()
conn.close()
print("\n🎯 QA Execution Completed Successfully ✅")
