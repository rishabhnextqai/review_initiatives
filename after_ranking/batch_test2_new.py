#!/usr/bin/env python
import subprocess
import os
import sys
import json
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from sqlalchemy import create_engine, text
import traceback
from datetime import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# ----------------------------------------------------------------------
# CONFIGURATION & PATHS

base_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LOGS_FOLDER = os.path.join(base_directory, 'logs')
os.makedirs(LOGS_FOLDER, exist_ok=True)

LOG_FILE = os.path.join(LOGS_FOLDER, f'logs_batch_{datetime.now().strftime("%Y%m%d")}.log')
TIMEOUT_SECONDS = 7200

EMAIL_TEMPLATES_PATH = os.path.join(base_directory, "DS", "ai_framework", "NextQSummary", "email_templates")
EMAIL_ASSETS_PATH = os.path.join(base_directory, "DS","ai_framework", "NextQSummary", "email_assets")



# ----------------------------------------------------------------------
# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

BASE_PATH = os.getcwd()
logging.info(f"BASE_PATH: {BASE_PATH}")

# ----------------------------------------------------------------------
# ERRORS

class CriticalError(Exception):
    pass

# ----------------------------------------------------------------------
# CONFIG LOADING

def load_config_files(path_feed: str):
    files = [
        ("config_question_template.json", "questions config"),
        ("config_inputs.json", "inputs config"),
        ("tenant_info.json", "tenant info"),
        ("config_db.json", "database config")
    ]
    configs = []
    for file, name in files:
        with open(os.path.join(path_feed, file), "r", encoding="utf-8") as f:
            cfg = json.load(f)
            if file == "tenant_info.json":
                cfg = cfg["tenant_info"][0]
            configs.append(cfg)
    return tuple(configs)

def load_configuration(path):
    try:
        config_questions, config_inputs, tenant_info, config_db = load_config_files(path)
        os.environ["OPENAI_API_KEY"] = str(config_inputs['openapi_key'])
        return {
            'questions': config_questions,
            'inputs': config_inputs,
            'tenant': tenant_info,
            'db': config_db
        }
    except Exception as e:
        raise CriticalError(f"Configuration error: {e}") from e

# ----------------------------------------------------------------------
# THREAD CAPACITY

def get_system_thread_capacity():
    try:
        with open('/proc/sys/kernel/threads-max', 'r') as f:
            max_threads = int(f.read().strip())
            logging.info(f"Max threads available (kernel): {max_threads}")
    except FileNotFoundError:
        max_threads = (os.cpu_count() or 1) * 100
        logging.error(f"/proc/sys/kernel/threads-max not found; using estimate {max_threads}")
    return min(max_threads, psutil.cpu_count() * 100)

# ----------------------------------------------------------------------
# DATABASE ENGINE

def get_db_engine(tenant_info):
    if not tenant_info:
        return None
    return create_engine(
        f'postgresql+psycopg2://{tenant_info["db_user"]}:{tenant_info["db_password"]}@'
        f'{tenant_info["db_host"]}:{tenant_info["db_port"]}/{tenant_info["db_name"]}'
    )

# ----------------------------------------------------------------------
# FETCH ACCOUNTS (with both ID and Name)

def get_account_data(db_engine, config_db):
    session_id = sys.argv[1]
    query = text(f"""
        SELECT 
            accountid,
            accountname
        FROM {config_db['dwh_schema']}.{config_db['input_account']}
        WHERE session_id = :sid
    """)
    with db_engine.connect() as cnx:
        return pd.read_sql(query, cnx, params={"sid": session_id})

# ----------------------------------------------------------------------
# UPDATE ACCOUNT STATUS ON FAILURE

def update_account_status(db_engine, account_id):
    try:
        with db_engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE dwh.d_input_account
                       SET s3_status = 'failed'
                     WHERE accountid = :aid AND s3_status IS NULL
                """),
                {"aid": account_id}
            )
            conn.execute(
                text("""
                    UPDATE dwh.d_input_account
                       SET report_status = 'failed'
                     WHERE accountid = :aid
                """),
                {"aid": account_id}
            )
    except Exception as e:
        logging.error(f"Error updating account status for {account_id}: {e}")
        logging.error(traceback.format_exc())

# ----------------------------------------------------------------------
# RUN SCRIPTS

def process_account(script_list, account_id, db_engine):
    script_path = os.path.join(BASE_PATH, "ai_framework", "NextQSummary", "scripts")
    os.chdir(script_path)
    log_path = os.path.join(BASE_PATH, "ai_framework", "logs", account_id)
    os.makedirs(log_path, exist_ok=True)

    account_status = {}
    for script in script_list:
        cmd = f'python {script} --account "{account_id}"'
        try:
            result = subprocess.run(
                cmd, shell=True, check=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, timeout=TIMEOUT_SECONDS
            )
            account_status[script] = "SUCCESS"
            with open(os.path.join(log_path, f"{script}_{account_id}.log"), "w") as f:
                f.write(f"SUCCESS\n{result.stdout}")
        except subprocess.TimeoutExpired:
            account_status[script] = "FAILED"
            update_account_status(db_engine, account_id)
            with open(os.path.join(log_path, f"ERROR_{script}_{account_id}.log"), "w") as f:
                f.write(f"FAILED (TIMEOUT)\nTerminated after {TIMEOUT_SECONDS}s.")
            logging.error(f"[TIMEOUT] {script} for account {account_id} exceeded {TIMEOUT_SECONDS}s.")
            break
        except subprocess.CalledProcessError as e:
            account_status[script] = "FAILED"
            update_account_status(db_engine, account_id)
            with open(os.path.join(log_path, f"ERROR_{script}_{account_id}.log"), "w") as f:
                f.write(f"FAILED\n{e.stderr}")
            logging.error(f"[ERROR] {script} for account {account_id} failed: {e.stderr}")
            break

        logging.info(f"{script} : {account_status[script]}")

    # if successful so far, push to S3 and run remaining scripts
    if all(state == "SUCCESS" for state in account_status.values()):
        for script in [
            "pushing_files_to_s3.py",
            "process_audio_briefing.py",
            "process_home_acc_kpi.py",
            "f_csm_review_sum.py",
            "f_cust_usage_stats.py"
        ]:
            s3_cmd = f'python {script} --account "{account_id}"'
            try:
                result = subprocess.run(
                    s3_cmd, shell=True, check=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, timeout=TIMEOUT_SECONDS
                )
                account_status[script] = "SUCCESS"
                with open(os.path.join(log_path, f"{script}_{account_id}.log"), "w") as f:
                    f.write(f"SUCCESS\n{result.stdout}")
            except subprocess.TimeoutExpired:
                account_status[script] = "FAILED"
                update_account_status(db_engine, account_id)
                with open(os.path.join(log_path, f"ERROR_{script}_{account_id}.log"), "w") as f:
                    f.write(f"FAILED (TIMEOUT)\nTerminated after {TIMEOUT_SECONDS}s.")
                logging.error(f"[TIMEOUT] {script} for account {account_id} exceeded {TIMEOUT_SECONDS}s.")
                break
            except subprocess.CalledProcessError as e:
                account_status[script] = "FAILED"
                update_account_status(db_engine, account_id)
                with open(os.path.join(log_path, f"ERROR_{script}_{account_id}.log"), "w") as f:
                    f.write(f"FAILED\n{e.stderr}")
                logging.error(f"[ERROR] {script} for account {account_id} failed: {e.stderr}")
                break

            logging.info(f"{script} : {account_status[script]}")

    return account_id, account_status

# ----------------------------------------------------------------------
# PERIODID UPDATE

def update_genai_periodid(db_engine, config_targetDB, session_id):
    schema = config_targetDB["dwh_schema"]
    rundate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with db_engine.connect() as cnx:
            cnx.execute(
                text(f"""
                    UPDATE {schema}.{config_targetDB["table_imperatives"]}
                       SET periodid = :rundate
                     WHERE (accountid, periodid) IN (
                          SELECT accountid, MAX(periodid)
                            FROM {schema}.{config_targetDB["table_imperatives"]}
                           WHERE accountid IN (
                                SELECT accountid
                                  FROM dwh.d_input_account
                                 WHERE session_id = :sid
                           )
                        GROUP BY accountid
                     )
                """),
                {"rundate": rundate, "sid": session_id}
            )
        logging.info("Updated periodid in GENAI table")
    except Exception as e:
        logging.error(f"Error in update_genai_periodid: {e}")
        logging.error(traceback.format_exc())

# ----------------------------------------------------------------------
# MAPPING SCRIPT SELECTION

def get_mapping_script_name():
    try:
        cwd = os.getcwd()
        sections_path = os.path.join(cwd, 'sections.json')
        if not os.path.exists(sections_path):
            return 'chatgpt_industrywise_initiative_mapping.py'
        with open(sections_path, 'r') as f:
            data = json.load(f)
        val = data[0].get('industry_specifc_recommendation')
        if str(val).lower() == 'yes':
            return 'chatgpt_industrywise_initiative_mapping.py'
        return 'chatgpt_initiative_mapping.py'
    except:
        return 'chatgpt_industrywise_initiative_mapping.py'

# ----------------------------------------------------------------------
# EMAIL NOTIFICATIONS

def get_notification_email(db_engine, email_id):
    """Get admin full name and email details for a specific email"""
    try:
        query = text("""
            SELECT DISTINCT email_id, first_name, last_name 
            FROM dwh.d_user_detl 
            WHERE LOWER(role) = 'admin' 
            AND LOWER(email_id) != 'csm@nextq.ai'
            AND email_id = :email_id
            """)

        df = pd.read_sql(query, db_engine, params={"email_id": email_id})
        if not df.empty:
            row = df.iloc[0]
            full_name = f"{row['first_name']} {row['last_name']}".strip()
            return {
                'email_id': row['email_id'],
                'admin_fullname': full_name if full_name != " " else "Admin"
            }
    except Exception as e:
        logging.error(f"Failed to fetch admin details for {email_id}: {e}")
    return {'email_id': email_id, 'admin_fullname': 'Admin'}

def get_all_notification_emails(db_engine):
    """Get all admin emails"""
    try:
        df = pd.read_sql(
            text("SELECT DISTINCT email_id FROM dwh.d_user_detl WHERE LOWER(role) = 'admin' AND LOWER(email_id) != 'csm@nextq.ai'"),
            db_engine
        )
        return df['email_id'].dropna().tolist()
    except Exception as e:
        logging.error(f"Failed to fetch notification emails: {e}")
        return []

def load_html_template(template_filename: str) -> str:
    """Load HTML template from file"""
    template_path = os.path.join(EMAIL_TEMPLATES_PATH, template_filename)
    if not os.path.isfile(template_path):
        logging.error(f"HTML template file not found: {template_path}")
        return f"<html><body>Critical Error: Email template '{template_filename}' not found.</body></html>"
    
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logging.error(f"Error reading template {template_path}: {e}")
        return f"<html><body>Error loading template: {e}</body></html>"

def send_html_email(recipients: list, subject: str, html_body: str, email_cfg: dict, cc: list = None, bcc: list = None):
    """Send HTML email with embedded images and optional CC/BCC"""
    if not recipients:
        logging.warning("No recipients for email; skipping send.")
        return

    msg_root = MIMEMultipart('related')
    msg_root['Subject'] = subject
    msg_root['From'] = email_cfg.get("EMAIL_SENDER", "")
    msg_root['To'] = ", ".join(recipients)
    if cc:
        msg_root['Cc'] = ", ".join(cc)

    msg_alternative = MIMEMultipart('alternative')
    msg_root.attach(msg_alternative)
    msg_alternative.attach(MIMEText(html_body, 'html'))

    images_to_embed = [
        ('nextqlogo3.png', 'nextqlogo3'),
        ('nextqlogo2.png', 'nextqlogo2'),
        ('ellipse20.png', 'ellipse20')
    ]

    for img_filename, img_cid in images_to_embed:
        img_path = os.path.join(EMAIL_ASSETS_PATH, img_filename)
        try:
            with open(img_path, 'rb') as img_file:
                img = MIMEImage(img_file.read())
                img.add_header('Content-ID', f'<{img_cid}>')
                img.add_header('Content-Disposition', 'inline', filename=img_filename)
                msg_root.attach(img)
        except FileNotFoundError:
            logging.error(f"Image not found for embedding: {img_path}")
        except Exception as e:
            logging.error(f"Failed to embed image {img_path}: {e}")

    try:
        all_recipients = list(set(recipients + (cc or []) + (bcc or [])))
        with smtplib.SMTP(email_cfg.get("SMTP_SERVER", ""), int(email_cfg.get("SMTP_PORT", 587))) as server:
            server.starttls()
            server.login(email_cfg.get("EMAIL_SENDER", ""), email_cfg.get("EMAIL_PASSWORD", ""))
            server.send_message(msg_root, to_addrs=all_recipients)
            logging.info(f"HTML email sent to: {recipients}, CC: {cc}, BCC: {bcc}")
    except Exception as e:
        logging.error(f"Failed to send HTML email: {e}")
        logging.error(traceback.format_exc())



def determine_email_type(account_status):
    """Determine which email template to use based on script results"""
    # Check if all scripts succeeded
    if all(status == "SUCCESS" for status in account_status.values()):
        return "success"
    
    # Check if only process_audio_briefing.py failed
    failed_scripts = [script for script, status in account_status.items() if status == "FAILED"]
    
    #if len(failed_scripts) == 1 and "process_audio_briefing.py" in failed_scripts:
    #    return "almost"
    
    # Any other failure case
    return "failed"


def build_review_table(df):
    if df.empty:
        return "<p>No pending or rejected reviews at this time.</p>"

    # Capitalize all text columns and format date
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.title()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime("%d %b %Y")

    html = """<table border="1" cellpadding="6" cellspacing="0" 
       style="border-collapse: collapse; font-family: 'Roboto-Regular', Helvetica, Arial, sans-serif; font-size: 14px; width: 100%; table-layout: fixed; word-wrap: break-word;">
  <thead style="background-color: #00d651; color: white;">
    <tr>
      <th style="text-align: left;">Customer Name</th>
      <th style="text-align: left;">Account Name</th>
      <th style="text-align: left;">NQ Research</th>
      <th style="text-align: left;">Sales Playbook</th>
      <th style="text-align: left;">Audio Briefings</th>
      <th style="text-align: left;">Date</th>
    </tr>
  </thead>
  <tbody>

    """

    for _, row in df.iterrows():
        html += "<tr>"
        html += f"<td>{row['Customer Name']}</td>"
        html += f"<td>{row['Account Name']}</td>"
        html += f"<td>{row['NQ Research']}</td>"
        html += f"<td>{row['Sales Playbook']}</td>"
        html += f"<td>{row['Audio Briefings']}</td>"
        html += f"<td>{row['Date']}</td>"
        html += "</tr>"

    html += "</tbody></table>"
    return html


def build_failed_table(db_engine, config):
    try:
        # Connect to master DB
        with db_engine.connect() as cnx:
            result = cnx.execute(text("SELECT db_name FROM dwh.d_master_db LIMIT 1")).fetchone()
            master_dbname = result[0] if result else None

        if not master_dbname:
            logging.error("Master DB name not found.")
            return "<p>Failed data unavailable.</p>"

        master_engine = create_engine(
            f'postgresql+psycopg2://{config["tenant"]["db_user"]}:{config["tenant"]["db_password"]}@'
            f'{config["tenant"]["db_host"]}:{config["tenant"]["db_port"]}/{master_dbname}'
        )

        failed_query = text("""
            SELECT customer_name AS "Customer Name",
                   accountname AS "Account Name",
                   failed_date AS "Date"
              FROM staging.f_failed_accounts
        """)

        df = pd.read_sql(failed_query, master_engine)

        if df.empty:
            return "<p>No failed accounts at this time.</p>"

        df = df.fillna("�")
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce').dt.strftime("%d %b %Y")
        df["Account Name"] = df["Account Name"].astype(str).str.strip()

        html = """
        <table border="1" cellpadding="6" cellspacing="0"
               style="border-collapse: collapse; font-family: 'Roboto-Regular', Helvetica, Arial, sans-serif; width: 100%; font-size: 14px;">
            <thead style="background-color: #00d651; color: white;">
                <tr>
                    <th style="border: 1px solid #ddd; padding: 8px;text-align: left;">Customer Name</th>
                    <th style="border: 1px solid #ddd; padding: 8px;text-align: left;">Account Name</th>
                    <th style="border: 1px solid #ddd; padding: 8px;text-align: left;">Date</th>
                </tr>
            </thead>
            <tbody>
        """

        for _, row in df.iterrows():
            html += "<tr>"
            html += f"<td style='border: 1px solid #ddd; padding: 8px;text-align: left'>{row['Customer Name']}</td>"
            html += f"<td style='border: 1px solid #ddd; padding: 8px;text-align: left;'>{row['Account Name']}</td>"
            html += f"<td style='border: 1px solid #ddd; padding: 8px;text-align: left;'>{row['Date']}</td>"
            html += "</tr>"

        html += "</tbody></table>"
        return html

    except Exception as e:
        logging.error(f"Error generating failed accounts table: {e}")
        return "<p>Error loading failed accounts data.</p>"

def send_account_notification(db_engine, email_cfg, account_id, account_name, account_status, config):
    """Send notification for a specific account, with report approval logic"""
    try:
        email_type = determine_email_type(account_status)

        # Read approval flag from sections.json
        sections_path = os.path.abspath(os.path.join(base_directory, 'DS', 'sections.json'))
        approval_required = "no"
        try:
            with open(sections_path, 'r') as f:
                data = json.load(f)
                approval_required = str(data[0].get("report_approval_requirement", "no")).lower()
        except Exception as e:
            logging.warning(f"Could not read report_approval_requirement from sections.json: {e}")

        if email_type == "success":
            if approval_required == "yes":
                recipients = ['csm@nextq.ai', 'ai@nextq.ai']
                cc = []
                bcc = []
                template_file = "hurraycsm.html"
                subject = f"Q-Pilot: Report Pending Approval Summary"
            else:
                # Get user emails
                user_emails = []
                try:
                    query_user_ids = text("""
                        SELECT DISTINCT user_id
                        FROM dwh.f_user_account
                        WHERE accountid = :aid
                    """)
                    df_user_ids = pd.read_sql(query_user_ids, db_engine, params={"aid": account_id})
                    user_ids = df_user_ids['user_id'].dropna().tolist()

                    if user_ids:
                        query_emails = text("""
                            SELECT DISTINCT email_id
                            FROM dwh.d_user_detl
                            WHERE user_id = ANY(:user_ids)
                              AND email_id IS NOT NULL
                        """)
                        df_emails = pd.read_sql(query_emails, db_engine, params={"user_ids": user_ids})
                        user_emails = df_emails['email_id'].dropna().astype(str).str.strip().tolist()
                except Exception as e:
                    logging.warning(f"Error fetching user emails for account {account_id}: {e}")

                # Get admin emails
                admin_emails = get_all_notification_emails(db_engine)
                admin_emails = [e.strip() for e in admin_emails if e.strip()]

                user_set = set(user_emails)
                admin_set = set(admin_emails)

                to_emails = sorted(user_set - admin_set)
                cc_emails = sorted(admin_set)

                # If TO is empty, move all admin to TO instead
                if not to_emails:
                    to_emails = cc_emails
                    cc_emails = []

                bcc_emails = ['csm@nextq.ai', 'ai@nextq.ai']
                bcc_emails = [b for b in bcc_emails if b not in to_emails and b not in cc_emails]

                recipients = to_emails
                cc = cc_emails
                bcc = bcc_emails
                template_file = "hurray2.html"
                subject = f"Q-Pilot: Account {account_name} Completed"

        elif email_type == "failed":
            failure_recovery_scripts = ["failed_accounts_update.py", "f_cust_usage_stats.py"]
            script_path = os.path.join(BASE_PATH, "ai_framework", "NextQSummary", "scripts")
            log_path = os.path.join(BASE_PATH, "ai_framework", "logs", account_id)
            os.makedirs(log_path, exist_ok=True)
            os.chdir(script_path)
            for recovery_script in failure_recovery_scripts:
                try:
                    recovery_cmd = f'python {recovery_script} --account "{account_id}"'
                    result = subprocess.run(
                        recovery_cmd, shell=True, check=True,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True, timeout=TIMEOUT_SECONDS
                    )
                    logging.info(f"[RECOVERY] {recovery_script} succeeded for {account_id}")
                    with open(os.path.join(log_path, f"{recovery_script}_{account_id}.log"), "w") as f:
                        f.write(f"SUCCESS\n{result.stdout}")
                except subprocess.CalledProcessError as e:
                    logging.error(f"[RECOVERY] {recovery_script} failed for {account_id}: {e.stderr}")
                    with open(os.path.join(log_path, f"ERROR_{recovery_script}_{account_id}.log"), "w") as f:
                        f.write(f"FAILED\n{e.stderr}")
                except subprocess.TimeoutExpired:
                    logging.error(f"[RECOVERY TIMEOUT] {recovery_script} for account {account_id}")
                    with open(os.path.join(log_path, f"ERROR_{recovery_script}_{account_id}.log"), "w") as f:
                        f.write(f"FAILED (TIMEOUT)\nTerminated after {TIMEOUT_SECONDS}s.")
            recipients = ['csm@nextq.ai', 'ai@nextq.ai']
            cc = []
            bcc = []
            template_file = "failed.html"
            subject = f"Email for Failed Report Generation"
        else:
            logging.info(f"Skipping email for account {account_id} due to unsupported email type: {email_type}")
            return

        # Load and personalize HTML
        html_template = load_html_template(template_file)
        personalized_html = html_template.replace("{accountname}", account_name)
        
        if template_file == "failed.html":
            failed_html = build_failed_table(db_engine, config)
            personalized_html = personalized_html.replace("[failed_table]", failed_html)
        
        if "hurray" in template_file and "{admin}" in personalized_html:
            admin_info = get_notification_email(db_engine, recipients[0] if recipients else '')
            personalized_html = personalized_html.replace("{admin}", admin_info.get("admin_fullname", "Admin"))

        # Inject approval review table
        if approval_required == "yes" and template_file == "hurraycsm.html":
            try:
                with db_engine.connect() as cnx:
                    result = cnx.execute(text("SELECT db_name FROM dwh.d_master_db LIMIT 1")).fetchone()
                    master_dbname = result[0] if result else None

                if master_dbname:
                    master_engine = create_engine(
                        f'postgresql+psycopg2://{config["tenant"]["db_user"]}:{config["tenant"]["db_password"]}@'
                        f'{config["tenant"]["db_host"]}:{config["tenant"]["db_port"]}/{master_dbname}'
                    )

                    review_query = text("""
                        SELECT customer_id, accountname, ai_report_review_status, 
                               play_book_review_status, audio_briefing_review_status, 
                               submitted_for_approval_dt
                        FROM staging.f_csm_review_sum
                        WHERE ai_report_review_status ILIKE ANY (ARRAY['pending', 'rejected'])
                           OR play_book_review_status ILIKE ANY (ARRAY['pending', 'rejected'])
                           OR audio_briefing_review_status ILIKE ANY (ARRAY['pending', 'rejected'])
                    """)
                    df_review = pd.read_sql(review_query, master_engine)

                    csr_query = text("SELECT customer_id, customer_name FROM staging.csr_master")
                    df_csr = pd.read_sql(csr_query, master_engine)

                    df_review = df_review.merge(df_csr, on="customer_id", how="left")
                    df_review["customer_id"] = df_review["customer_name"]
                    df_review = df_review.drop(columns=["customer_name"])

                    df_review = df_review.rename(columns={
                        "customer_id": "Customer Name",
                        "accountname": "Account Name",
                        "ai_report_review_status": "NQ Research",
                        "play_book_review_status": "Sales Playbook",
                        "audio_briefing_review_status": "Audio Briefings",
                        "submitted_for_approval_dt": "Date"
                    })

                    df_review = df_review.sort_values(by="Date", ascending=False)
                    df_review["Date"] = pd.to_datetime(df_review["Date"]).dt.strftime("%d %b %Y")
                    review_html = build_review_table(df_review)
                    personalized_html = personalized_html.replace("[REVIEW]", review_html)

            except Exception as e:
                logging.error(f"Error building CSM review table: {e}")
                personalized_html = personalized_html.replace("[REVIEW]", "<p>Review data unavailable.</p>")

        # Send email
        send_html_email(recipients, subject, personalized_html, email_cfg, cc=cc, bcc=bcc)
        logging.info(f"Sent '{email_type}' email for {account_name} to {recipients} with CC {cc} and BCC {bcc}")

    except Exception as e:
        logging.error(f"Error sending email for account {account_id}: {e}")
        logging.error(traceback.format_exc())




# ----------------------------------------------------------------------
# MAIN ENTRYPOINT

def main():
    if len(sys.argv) < 2:
        print("Usage: python batch_processor.py <session_id>")
        sys.exit(1)
    session_id = sys.argv[1]
    logging.info(f"Session ID: {session_id}")

    # Load configs and DB engine
    path_feed = os.path.join(BASE_PATH, "ai_framework", "NextQSummary", "feed")
    config = load_configuration(path_feed)
    db_engine = get_db_engine(config['tenant'])

    # Fetch accounts with names
    df_acc = get_account_data(db_engine, config['db'])
    accounts_info = df_acc.to_dict('records')
    account_ids   = [rec['accountid'] for rec in accounts_info]

    max_threads = get_system_thread_capacity()
    num_threads = min(max_threads, len(account_ids), 100)

    mapping_script = get_mapping_script_name()
    logging.info(f"Selected mapping script: {mapping_script}")

    SCRIPTS = [
        # # #"f_cust_usage_stats.py"
        # # "process_keycontact_extraction.py",
        # "process_jobdata_extraction.py",
        # "leadership.py",
        # "chatgpt_get_personatitle_and_keypersonname.py",
        # "Earnings_call.py",
        # "MDA.py",
        # "persona_articles1.py",
        # "webzio_script.py",
        # #"chatgpt_get_initiatives_from_multiple_sources.py",
        # "initiative_pipeline.py",
        # "merge_initiatives1.py",
        # "rank_initiatives1.py",
        # "process_recommendation.py",
        # #mapping_script,
        # "chatgpt_industrywise_initiative_mapping.py",
        # "mapping_update_doclink.py",
        # "chatgpt_recommendation_A_and_B.py",
        # "chatgpt_get_personatitle_update.py",
        # "chatgpt_intent_persona.py",
        # "chatgpt_get_job_signal.py",
        # "rank_initiatives_final.py",
        "updating_main_table_2.py",
        "chatgpt_nextqsummary_and_update_docpath.py",
        "Initiatives_1.py",
        "appendix_temp2.py",
        "add_financial_summary_A.py",
        "call_to_action_A.py",
        "themes.py",
        "persona_initiative2.py",
        "makeitbold.py",
        "process_realtime.py",
        "process_keycontact.py",
        "process_com.py"
    ]

    logging.info(f"Processing {len(account_ids)} accounts with {num_threads} threads")
    account_results = {}

    def process_accounts(ids):
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {
                executor.submit(process_account, SCRIPTS, aid, db_engine): aid
                for aid in ids
            }
            completed = 0
            for fut in as_completed(futures):
                aid = futures[fut]
                try:
                    _, status = fut.result()
                    account_results[aid] = status
                    completed += 1
                    logging.info(f"Account {aid} completed ({completed}/{len(ids)})")
                    
                    # Update database status based on results
                    email_type = determine_email_type(status)
                    # if email_type == "success":
                    #     update_account_status_success(db_engine, aid)
                    # For failed and almost, account status is already updated in process_account
                        
                except Exception as e:
                    logging.error(f"Critical error on account {aid}: {e}")
                    account_results[aid] = {"CRITICAL_ERROR": "FAILED"}

    # First pass
    process_accounts(account_ids)

    # Update periodid
    try:
        update_genai_periodid(db_engine, config['db'], session_id)
    except Exception as e:
        logging.error(f"Error updating periodid: {e}")

    # Send per-account notifications
    try:
        email_cfg  = config['inputs'].get('email', {})
     
        # append your two fixed addresses


        for rec in accounts_info:
            acct_id   = rec['accountid']
            acct_name = rec['accountname']
            
            if acct_id in account_results:
                account_status = account_results[acct_id]
                send_account_notification(db_engine, email_cfg, acct_id, acct_name, account_status, config)
            else:
                logging.warning(f"No results found for account {acct_id}")

         

    except Exception as e:
        logging.error(f"Error sending per-account notifications: {e}")
        logging.error(traceback.format_exc())

    # Final log
    failed_accounts = [aid for aid, status in account_results.items() 
                      if any(s == "FAILED" for s in status.values())]
    
    if failed_accounts:
        logging.info(f"Accounts with failures: {failed_accounts}")
    else:
        logging.info("All accounts processed successfully.")

if __name__ == "__main__":
    main()