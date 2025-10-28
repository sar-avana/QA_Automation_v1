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

# SQL templates executed once per DB.Schema
sql_files = ["qa_date_range.sql", "qa_duplicate.sql", "qa_rowcount.sql"]

# Data structures
qa_summary_list = []
qa_detailed_grouped = {}
DELTA_THRESHOLD = 2

# -------------------------
# Load config & connect
# -------------------------
config_df = pd.read_csv("db_schema_config.csv")
if "Status" not in config_df.columns:
    config_df["Status"] = ""

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
# Main loop: one QA per DB.Schema
# -------------------------
for idx, row in config_df.iterrows():
    db = row['Database']
    schema = row['Schema']
    status_flag = row.get('Status', "")
    owner_email = str(row.get("Email", "") or "").strip()

    key = f"{db}_{schema}"
    print(f"\n📌 Processing: {db}.{schema}")

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
    review_table_str = ",".join(latest_tables.values()) if latest_tables else ""
    latest_table_str = ",".join(prev_tables.values()) if prev_tables else ""

    # Execute each SQL file exactly once per schema.
    # SQL files are expected to use {{DB}} and {{SCHEMA}} placeholders (and other placeholders are replaced with empty strings to be safe).
    for sql_file in sql_files:
        try:
            with open(sql_file, 'r') as f:
                raw_sql = f.read()
        except Exception as e:
            print(f"⚠️ Failed to read {sql_file}: {e}")
            qa_detailed_grouped[key]["details"].append({
                "database": db,
                "schema": schema,
                "qa_table": "",
                "prev_table": "",
                "sql_file": sql_file,
                "error": f"Failed to read SQL file: {e}",
                "checked_at": datetime.now()
            })
            continue

        # Replace placeholders: core replacements kept, table placeholders replaced with empty string
        sql_query = (raw_sql
                     .replace("{{DB}}", db)
                     .replace("{{SCHEMA}}", schema)
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
                # preserve existing fields & add provenance; tablename set to schema-level
                entry = {
                    **row_dict,
                    "database": db,
                    "schema": schema,
                    "tablename": f"{db}.{schema}",   # schema-level provenance
                    "qa_table": "",
                    "prev_table": "",
                    "sql_file": sql_file,
                    "checked_at": datetime.now()
                }
                qa_detailed_grouped[key]["details"].append(entry)

        except Exception as e:
            print(f"⚠️ SQL Error in {sql_file} for schema {db}.{schema}: {e}")
            qa_detailed_grouped[key]["details"].append({
                "database": db,
                "schema": schema,
                "tablename": f"{db}.{schema}",
                "qa_table": "",
                "prev_table": "",
                "sql_file": sql_file,
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

# Process metrics and alerts and send email per DB.Schema
for key, group in qa_detailed_grouped.items():
    detail_df = pd.DataFrame(group["details"])
    detail_df.to_csv(f"{key}.csv", index=False)
    print(f"📌 Detailed QA saved → {key}.csv")

    alerts = []
    service_latest = service_prev = None
    fill_latest = fill_prev = None

    for _, r in detail_df.iterrows():
        test = r.get('TEST')
        result = r.get('RESULT')
        table_name = r.get('tablename', r.get('TABLE_NAME', key))

        # Service Date stale check
        if test == 'MAX SERVICE_DATE LATEST':
            service_latest = pd.to_datetime(result, errors='coerce').date()
        elif test == 'MAX SERVICE_DATE PREVIOUS':
            service_prev = pd.to_datetime(result, errors='coerce').date()

        if service_latest and service_prev:
            if service_latest == service_prev:
                alerts.append({
                    "alert_type": "SERVICE DATE STALE",
                    "message": f"Service Date SAME → {service_latest}",
                    "table_name": table_name,
                    "timestamp": datetime.now()
                })
            service_latest = service_prev = None

        # Fill Date stale check
        if test == 'MAX FILL_DATE LATEST':
            fill_latest = pd.to_datetime(result, errors='coerce').date()
        elif test == 'MAX FILL_DATE PREVIOUS':
            fill_prev = pd.to_datetime(result, errors='coerce').date()

        if fill_latest and fill_prev:
            if fill_latest == fill_prev:
                alerts.append({
                    "alert_type": "FILL DATE STALE",
                    "message": f"Fill Date SAME → {fill_latest}",
                    "table_name": table_name,
                    "timestamp": datetime.now()
                })
            fill_latest = fill_prev = None

    # Duplicate & Delta checks (same as before)
    dup_cols = [("PHARMACY_DUPLICATE_PERCENTAGE", "PHARMACY DUPLICATES"),
                ("MEDICAL_DUPLICATE_PERCENTAGE", "MEDICAL DUPLICATES")]
    for col, alert_type in dup_cols:
        if col in detail_df.columns:
            detail_df[col] = pd.to_numeric(detail_df[col], errors='coerce')
            dup = detail_df[detail_df[col] > 0]
            for _, r in dup.iterrows():
                alerts.append({
                    "alert_type": alert_type,
                    "message": f"Duplicates detected → {r[col]:.2f}%",
                    "table_name": r.get('tablename', r.get('TABLE_NAME','N/A')),
                    "timestamp": datetime.now()
                })

    delta_cols = [("ROW_DELTA_PCT", "ROW DELTA"), ("PATIENT_DELTA_PCT", "PATIENT DELTA")]
    for col, alert_type in delta_cols:
        if col in detail_df.columns:
            detail_df[col] = pd.to_numeric(detail_df[col], errors='coerce')
            delta = detail_df[detail_df[col].abs() > DELTA_THRESHOLD]
            for _, r in delta.iterrows():
                alerts.append({
                    "alert_type": alert_type,
                    "message": f"{alert_type} > ±{DELTA_THRESHOLD}% → {r[col]:.2f}%",
                    "table_name": r.get('tablename', r.get('TABLE_NAME','N/A')),
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
