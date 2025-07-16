from io import BytesIO
import json
import os
import shutil
import subprocess
import threading
import select
import time
import logging
from datetime import datetime
from pathlib import Path

from fastapi.responses import FileResponse

from dotenv import load_dotenv  # type: ignore
load_dotenv()

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# Logging Configuration
script_dir = os.path.dirname(os.path.abspath(__file__))

# Configure logging to output to both console and a file in the script's directory
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.DEBUG,  # Set to DEBUG for extensive logging
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler(os.path.join(script_dir, "csm_backend_server_logs.txt"))  # File output
    ]
)
logger = logging.getLogger("fastapi_server")

# State Persistence
STATE_FILE = Path(script_dir) / "server_state.json"

def save_state(ds_root, db_name, account):
    state = {"ds_root": ds_root, "db_name": db_name, "account": account}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)
    logger.info(f"Saved state to {STATE_FILE}")

def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

# Env / Secrets
DB_HOST = os.getenv("PG_HOST")
DB_PORT = int(os.getenv("PG_PORT", "5432"))
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")

SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",")

RM_API_KEY = os.getenv("RM_API_KEY")

# Security
bearer = HTTPBearer()
def verify_token(creds: HTTPAuthorizationCredentials = Depends(bearer)):
    logger.debug(f"Verifying token: {creds.credentials[:5]}...")  # Log partial token for security
    if creds.credentials != RM_API_KEY:
        logger.warning("Invalid API key attempted")
        raise HTTPException(status_code=403, detail="Invalid API key")
    logger.info("API key verified successfully")

# FastAPI instance
app = FastAPI()

# Database config builder
def get_db_conn(db_name: str = None):
    """Get database connection with optional db_name parameter"""
    target_db = db_name if db_name else getattr(app.state, 'db_name', None)
    if not target_db:
        state = load_state()
        target_db = state.get("db_name")
        if not target_db:
            logger.error("DB_NAME not set; call /api/setup first")
            raise HTTPException(status_code=400, detail="DB_NAME not set; call /api/setup first")
        app.state.db_name = target_db
    logger.debug(f"Attempting to connect to database: {target_db}")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=target_db
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info(f"Successfully connected to database: {target_db}")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed for {target_db}: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# LISTEN/NOTIFY Setup
DB_SCHEMA = "dwh"
PRIMARY_TABLE = "f_ranked_initiatives"
PRIMARY_CHANNEL = "table_change_channel"
SECONDARY_TABLE = "f_genai_extracted_solvedchallenges"
SECONDARY_CHANNEL = "table_change_channel_secondary"

Q_PRIMARY = (
    f"SELECT accountname, initiativename, initiative "
    f"FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
    f"WHERE accountname = %s AND periodid = ("
    f"  SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname = %s"
    f") ORDER BY rank"
)
Q_SECONDARY = (
    f"SELECT DISTINCT product "
    f"FROM {DB_SCHEMA}.{SECONDARY_TABLE} "
    f"WHERE product IS NOT NULL AND product <> 'No Data';"
)

OUT_PRIMARY = "initiatives.xlsx"
OUT_SECONDARY = "offerings_products.xlsx"

# Operation-script mapping: folder name and file name
OPS = {
    "generate till initiatives":  ("until_initiatives",  "batch_test2_new.py"),
    "generate only initiatives":  ("just_initiatives",   "batch_test2_new.py"),
    "generate ranking":           ("initiatives_rank",   "batch_test2_new.py"),
    "generate reports":           ("after_ranking",      "batch_test2_new.py"),
    "Nothing": ("nothing", "batch_test2_new.py")}
REPO_BASE = Path(__file__).parent

# Helpers
def ensure_trigger(conn, table, channel):
    fn = f"notify_{table}_change"
    trg = f"watch_{table}_change"
    logger.debug(f"Ensuring trigger for table: {table}, channel: {channel}")
    with conn.cursor() as cur:
        cur.execute(sql.SQL("""
            CREATE OR REPLACE FUNCTION {fn}() RETURNS TRIGGER AS $$
            BEGIN PERFORM pg_notify(%s, ''); RETURN NEW; END;
            $$ LANGUAGE plpgsql;
        """).format(fn=sql.Identifier(fn)), [channel])
        logger.info(f"Created or replaced function: {fn}")
        cur.execute(sql.SQL("DROP TRIGGER IF EXISTS {trg} ON {sch}.{tbl};").format(
            trg=sql.Identifier(trg),
            sch=sql.Identifier(DB_SCHEMA),
            tbl=sql.Identifier(table)
        ))
        logger.debug(f"Dropped existing trigger: {trg}")
        cur.execute(sql.SQL("""
            CREATE TRIGGER {trg}
            AFTER INSERT OR UPDATE ON {sch}.{tbl}
            FOR EACH ROW EXECUTE PROCEDURE {fn}();
        """).format(
            trg=sql.Identifier(trg),
            sch=sql.Identifier(DB_SCHEMA),
            tbl=sql.Identifier(table),
            fn=sql.Identifier(fn)
        ))
        logger.info(f"Created trigger: {trg}")
    conn.commit()

def send_email(path):
    msg = MIMEMultipart()
    msg["Subject"] = f"Automated Report: {Path(path).name}"
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)
    msg.attach(MIMEText("Please find attached.", "plain"))
    with open(path, "rb") as f:
        part = MIMEApplication(f.read(), Name=Path(path).name)
        part["Content-Disposition"] = f'attachment; filename="{Path(path).name}"'
        msg.attach(part)
    logger.debug(f"Sending email to: {EMAIL_RECIPIENTS}, subject: {msg['Subject']}")
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.sendmail(SMTP_USER, EMAIL_RECIPIENTS, msg.as_string())
        logger.info(f"Email sent successfully: {path}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise

def listen_and_process(table, channel, query, out_file, stop_evt, label):
    thread_name = threading.current_thread().name
    logger.info(f"[{thread_name}] Starting listener for {label}")
    while not stop_evt.is_set():
        try:
            conn = get_db_conn()
            ensure_trigger(conn, table, channel)
            cur = conn.cursor()
            cur.execute(sql.SQL("LISTEN {ch};").format(ch=sql.Identifier(channel)))
            logger.debug(f"[{thread_name}] Listening on channel: {channel}")
            while not stop_evt.is_set():
                if select.select([conn], [], [], 60) == ([], [], []):
                    logger.debug(f"[{thread_name}] No notification received, continuing wait")
                    continue
                conn.poll()
                while conn.notifies:
                    conn.notifies.pop(0)
                    logger.debug(f"[{thread_name}] Received notification on channel {channel}")
                    df = pd.read_sql(query, conn, params=(app.state.account, app.state.account))
                    out = Path(app.state.ds_root) / out_file
                    df.to_excel(out, index=False)
                    logger.info(f"[{thread_name}] Generated Excel file: {out}")
                    send_email(str(out))
            conn.close()
            logger.info(f"[{thread_name}] Connection closed for {label}")
        except Exception as e:
            logger.exception(f"[{thread_name}] Exception in listener {label}: {e}")
            time.sleep(5)

def update_ranks(rows):
    account = rows[0]["accountname"]
    logger.debug(f"Updating ranks for account: {account}, rows: {len(rows)}")
    conn = get_db_conn()
    cur = conn.cursor()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bk = f"{DB_SCHEMA}.{PRIMARY_TABLE}_backup_{ts}"
    cur.execute(f"CREATE TABLE {bk} AS SELECT * FROM {DB_SCHEMA}.{PRIMARY_TABLE};")
    logger.info(f"Created backup table: {bk}")
    cur.execute(
        f"SELECT MAX(periodid) FROM {DB_SCHEMA}.{PRIMARY_TABLE} WHERE accountname=%s;",
        (account,)
    )
    maxp = cur.fetchone()[0]
    logger.debug(f"Max periodid for {account}: {maxp}")
    if maxp is None:
        logger.error(f"No data found for account: {account}")
        raise HTTPException(status_code=404, detail="No data")
    cur.execute(
        f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=NULL WHERE accountname=%s AND periodid=%s;",
        (account, maxp)
    )
    logger.debug(f"Cleared ranks for account: {account}, periodid: {maxp}")
    for r in rows:
        cur.execute(
            f"UPDATE {DB_SCHEMA}.{PRIMARY_TABLE} SET rank=%s "
            f"WHERE accountname=%s AND initiativename=%s AND periodid=%s;",
            (r["rank"], account, r["initiativename"], maxp)
        )
        logger.debug(f"Updated rank for initiativename: {r['initiativename']}")
    cur.execute(
        f"DELETE FROM {DB_SCHEMA}.{PRIMARY_TABLE} "
        f"WHERE accountname=%s AND periodid=%s AND rank IS NULL;",
        (account, maxp)
    )
    logger.debug(f"Deleted unranked rows for account: {account}, periodid: {maxp}")
    conn.close()
    logger.info(f"Ranks update completed for account: {account}")
    return [account]


@app.post("/api/setup", dependencies=[Depends(verify_token)])
async def api_setup(
    ds_path:   str = Form(...),   # e.g. /mnt/data/DS/ai_framework/NextQSummary/scripts
    operation: str = Form(...),
    account:   str = Form(...)
):
    global DS_ROOT, UPLOAD_DIR, DB_NAME

    norm = Path(ds_path).expanduser()
    if not norm.is_dir():
        raise HTTPException(status_code=400, detail="Provided ds_path does not exist or is not a directory")

    parts = norm.parts
    if "DS" not in parts:
        raise HTTPException(status_code=400, detail="ds_path must include 'DS' folder")

    # Exact upload/script directory
    UPLOAD_DIR = norm
    # DS_ROOT up through 'DS'
    DS_ROOT    = Path(*parts[: parts.index("DS") + 1])
    # DB_NAME from qpilot_v* segment
    DB_NAME    = next((p for p in parts if p.lower().startswith("qpilot_v")), None)
    if not DB_NAME:
        raise HTTPException(status_code=400, detail="Cannot derive DB_NAME from ds_path")

    # Copy & overwrite the selected operation script
    folder, fname = OPS.get(operation, (None, None))
    if not folder:
        raise HTTPException(status_code=400, detail="Invalid operation")
    src = REPO_BASE / folder / fname
    if not src.is_file():
        raise HTTPException(status_code=500, detail=f"Script not found at {src}")
    dst = DS_ROOT / fname
    if dst.exists(): dst.unlink()
    shutil.copy2(src, dst)
    logger.info(f"Deployed script {src.name} → {dst}")

    # Start listeners once
    if not getattr(app.state, "listeners_started", False):
        stop_evt = threading.Event()
        threading.Thread(
            target=listen_and_process,
            args=(PRIMARY_TABLE, PRIMARY_CHANNEL, Q_PRIMARY, OUT_PRIMARY, stop_evt, "Primary"),
            daemon=True
        ).start()
        threading.Thread(
            target=listen_and_process,
            args=(SECONDARY_TABLE, SECONDARY_CHANNEL, Q_SECONDARY, OUT_SECONDARY, stop_evt, "Secondary"),
            daemon=True
        ).start()
        app.state.listeners_started = True
        logger.info("Listeners launched for primary and secondary channels")

        app.state.ds_root = str(DS_ROOT)
        app.state.db_name = DB_NAME
        app.state.account = account
    return {"success": True, "message": f"Setup complete – script {fname} deployed to {DS_ROOT}"}

@app.post("/api/accountnames", dependencies=[Depends(verify_token)])
async def get_accounts(customer_id: str = Form(...)):
    db_name = f"qpilot_v1_{customer_id}"
    logger.debug(f"Retrieving accounts for customer_id: {customer_id}, db_name: {db_name}")
    try:
        conn = get_db_conn(db_name)
        query = "SELECT accountname FROM dwh.d_input_account;"
        df = pd.read_sql(query, conn)
        conn.close()
        if df.empty:
            logger.info("No accounts found")
            return {"accounts": ['N/A']}
        accounts = df["accountname"].dropna().drop_duplicates().tolist()
        logger.info(f"Retrieved {len(accounts)} accounts")
        app.state.db_name = db_name  # Set for consistency with /api/setup
        save_state(getattr(app.state, 'ds_root', None), db_name, getattr(app.state, 'account', None))
        return {"accounts": accounts}
    except Exception as e:
        logger.exception(f"Failed to retrieve accounts for customer_id={customer_id}")
        raise HTTPException(status_code=500, detail="Failed to retrieve account names")

@app.post("/api/upload", dependencies=[Depends(verify_token)])
async def api_upload(file: UploadFile = File(...)):
    if not hasattr(app.state, 'ds_root'):
        state = load_state()
        if not state.get("ds_root"):
            logger.error("app.state.ds_root not set; call /api/setup first")
            raise HTTPException(status_code=400, detail="Call `/api/setup` first")
        app.state.ds_root = state["ds_root"]
    outdir = Path(app.state.ds_root) / "uploads"
    outdir.mkdir(exist_ok=True, parents=True)
    dest = outdir / file.filename
    logger.debug(f"Uploading file to: {dest}")
    with open(dest, "wb") as f:
        f.write(await file.read())
    logger.info(f"File uploaded successfully: {file.filename}")
    return {"filename": file.filename}

def create_contact_json(ds_root: Path):
    """Create contact.json file in ds_root directory"""
    contact_data = {
        "status": "updated",
        "timestamp": pd.Timestamp.now().isoformat(),
        "message": "Contact file has been updated"
    }
    
    contact_json_path = ds_root / "contact.json"
    with open(contact_json_path, "w") as f:
        json.dump(contact_data, f, indent=2)
    logger.info(f"Created contact.json at: {contact_json_path}")

def comment_out_process_keycontact(ds_root: Path):
    """Comment out the process_keycontact_extraction.py line in batch_test2_new.py"""
    batch_file_path = ds_root / "batch_test2_new.py"
    
    if not batch_file_path.exists():
        logger.warning(f"batch_test2_new.py not found at: {batch_file_path}")
        return
    
    try:
        with open(batch_file_path, "r") as f:
            content = f.read()
        
        # Look for the specific keyword with double quotes
        target_line = '"process_keycontact_extraction.py"'
        
        # Split into lines and process
        lines = content.split('\n')
        modified = False
        
        for i, line in enumerate(lines):
            if target_line in line and not line.strip().startswith('#'):
                # Add # at the beginning of the line (preserving indentation)
                stripped = line.lstrip()
                indent = line[:len(line) - len(stripped)]
                lines[i] = indent + '#' + stripped
                modified = True
                logger.info(f"Commented out line: {line.strip()}")
        
        if modified:
            with open(batch_file_path, "w") as f:
                f.write('\n'.join(lines))
            logger.info(f"Successfully commented out process_keycontact_extraction.py in {batch_file_path}")
        else:
            logger.info("No uncommented lines with process_keycontact_extraction.py found")
            
    except Exception as e:
        logger.error(f"Failed to modify batch_test2_new.py: {str(e)}")

@app.post("/api/upload_contacts", dependencies=[Depends(verify_token)])
async def api_upload_contacts(
    file: UploadFile = File(...),
    account: str = Form(...)
):
    if not hasattr(app.state, 'ds_root'):
        state = load_state()
        if not state.get("ds_root"):
            logger.error("app.state.ds_root not set; call /api/setup first")
            raise HTTPException(status_code=400, detail="Call `/api/setup` first")
        app.state.ds_root = state["ds_root"]
    
    ds_root_path = Path(app.state.ds_root)
    outdir = ds_root_path / "ai_framework" / "NextQSummary" / "input" / "Coresignal" / "excel"
    outdir.mkdir(exist_ok=True, parents=True)
    
    if not file.filename.endswith(".csv"):
        logger.error("Only .csv files are supported")
        raise HTTPException(status_code=400, detail="Only .csv files are supported")
    
    dest = outdir / f"{account}.csv"
    logger.debug(f"Saving contact Excel to: {dest}")
    
    with open(dest, "wb") as f:
        f.write(await file.read())
    
    logger.info(f"Contact Excel uploaded and saved to: {dest}")
    
    # Create contact.json and comment out the line in batch_test2_new.py
    create_contact_json(ds_root_path)
    comment_out_process_keycontact(ds_root_path)
    
    return {"success": True, "saved_to": str(dest)}


# @app.post("/api/modify_contacts", dependencies=[Depends(verify_token)])
# async def api_modify_contacts(
#     file: UploadFile = File(...),
#     account: str = Form(...)
# ):
#     if not hasattr(app.state, 'ds_root'):
#         state = load_state()
#         if not state.get("ds_root"):
#             logger.error("app.state.ds_root not set; call /api/setup first")
#             raise HTTPException(status_code=400, detail="Call `/api/setup` first")
#         app.state.ds_root = state["ds_root"]

#     ds_root_path = Path(app.state.ds_root)
#     outdir = ds_root_path / "ai_framework" / "NextQSummary" / "input" / "Coresignal" / "excel"
#     outdir.mkdir(exist_ok=True, parents=True)
#     dest = outdir / f"{account}.xlsx"

#     logger.debug(f"Modifying contacts for account: {account}, destination: {dest}")

#     contents = await file.read()  # Read bytes from uploaded file
#     buffer = BytesIO(contents)

#     try:
#         if file.filename.endswith('.xlsx'):
#             new_df = pd.read_excel(buffer)
#         else:  # assume CSV
#             buffer.seek(0)
#             new_df = pd.read_csv(buffer)
#     except Exception as e:
#         logger.exception("Failed to read uploaded file")
#         raise HTTPException(status_code=400, detail=f"Failed to read file: {str(e)}")

#     file_updated = False
    
#     if dest.exists():
#         try:
#             existing_df = pd.read_excel(dest)
#             common_cols = list(set(existing_df.columns) & set(new_df.columns))
#             if not common_cols:
#                 logger.error("No matching columns found for modification")
#                 raise HTTPException(status_code=400, detail="No matching columns found")
#             combined_df = pd.concat([existing_df[common_cols], new_df[common_cols]], ignore_index=True)
#             combined_df.to_excel(dest, index=False)
#             logger.info(f"Appended to existing file: {dest}")
#             file_updated = True
#         except Exception as e:
#             logger.exception("Failed to modify existing file")
#             raise HTTPException(status_code=500, detail=f"Failed to modify file: {str(e)}")
#     else:
#         new_df.to_excel(dest, index=False)
#         logger.info(f"Created new file: {dest}")
#         file_updated = True

#     # Only create contact.json and comment out the line if file was actually updated
#     if file_updated:
#         create_contact_json(ds_root_path)
#         comment_out_process_keycontact(ds_root_path)

#     return {"filename": f"{account}.xlsx"}

# import pandas as pd
# import sqlite3
# import subprocess
# import threading
# import os
# from pathlib import Path
# from io import BytesIO
# from openpyxl import load_workbook
# from fastapi import HTTPException
# import logging

# logger = logging.getLogger(__name__)

# def get_account_id_from_db(account_name):
#     """Query d_input_account table to get account_id for given account_name"""
#     try:
#         conn = get_db_conn()  # Assuming this function is already available
#         cursor = conn.cursor()
        
#         # Query to get account_id based on account_name
#         cursor.execute(f"SELECT accountid FROM dwh.d_input_account WHERE accountname = '{account_name}'")
#         result = cursor.fetchone()
        
#         conn.close()
        
#         if result:
#             return result[0]
#         else:
#             logger.warning(f"Account ID not found for account name: {account_name}")
#             return None
#     except Exception as e:
#         logger.error(f"Error querying database for account_id: {str(e)}")
#         return None

# def convert_apexon_to_shell_format(apexon_df, account_id, account_name):
#     """Convert Apexon format dataframe to Shell format"""
#     try:
#         # Create new dataframe with shell format columns
#         shell_data = []
        
#         for _, row in apexon_df.iterrows():
#             # Mapping according to your requirements:
#             # Col A and B in shell - account_id and account_name (keep as is)
#             # Col C in shell - Col A + ' ' + Col B in apexon (First Name + ' ' + Last Name)
#             # Col D shell - Col A apexon (First Name)
#             # Col E shell - Col B in apexon (Last Name)
#             # Col F in shell - Col C apexon (Title)
#             # Col G in shell - Col AA apexon (Person Linkedin Url)
#             # Col H in shell - Col F apexon (Email)
#             # Col I in shell - Col AF + ', ' + AG + ', ' + AH in apexon (City, State, Country)
#             # Col J in shell - can be left blank for new modifications
#             # Col K in shell - Col C apexon (Title)
#             # Col L in shell - can be left blank for new entries
            
#             # Build location string (Col I) - ignore empty values
#             location_parts = []
#             city = str(row.get('City', '') or '').strip()
#             state = str(row.get('State', '') or '').strip()
#             country = str(row.get('Country', '') or '').strip()
            
#             if city:
#                 location_parts.append(city)
#             if state:
#                 location_parts.append(state)
#             if country:
#                 location_parts.append(country)
            
#             location = ', '.join(location_parts)
            
#             # Create the row data
#             shell_row = {
#                 'account_id': account_id,
#                 'account_name': account_name,
#                 'name': f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip(),
#                 'first_name': row.get('First Name', ''),
#                 'last_name': row.get('Last Name', ''),
#                 'title': row.get('Title', ''),
#                 'linkedin_url': row.get('Person Linkedin Url', ''),
#                 'email': row.get('Email', ''),
#                 'location': location,
#                 'photo_url': '',  # Col J - blank for new modifications
#                 'headline': row.get('Title', ''),  # Col K - same as title
#                 'professional_experience': ''  # Col L - blank for new entries
#             }
            
#             shell_data.append(shell_row)
        
#         # Create DataFrame with shell format
#         shell_df = pd.DataFrame(shell_data)
#         return shell_df
        
#     except Exception as e:
#         logger.error(f"Error converting Apexon to Shell format: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Format conversion failed: {str(e)}")

# # def delete_other_sheets(file_path, account_name):
# #     """Delete all sheets except the 'Apollo' sheet for the given account"""
# #     try:
# #         workbook = load_workbook(file_path)
        
# #         # Find the Apollo sheet name (it should contain the account name)
# #         apollo_sheet_name = None
# #         for sheet_name in workbook.sheetnames:
# #             if 'Apollo' in sheet_name and account_name in sheet_name:
# #                 apollo_sheet_name = sheet_name
# #                 break
        
# #         if not apollo_sheet_name:
# #             # If no exact match, look for just 'Apollo'
# #             for sheet_name in workbook.sheetnames:
# #                 if 'Apollo' in sheet_name:
# #                     apollo_sheet_name = sheet_name
# #                     break
        
# #         if apollo_sheet_name:
# #             # Delete all sheets except the Apollo one
# #             sheets_to_delete = [sheet for sheet in workbook.sheetnames if sheet != apollo_sheet_name]
# #             for sheet_name in sheets_to_delete:
# #                 del workbook[sheet_name]
# #                 logger.info(f"Deleted sheet: {sheet_name}")
# #         else:
# #             logger.warning(f"Apollo sheet not found for account: {account_name}")
        
# #         workbook.save(file_path)
# #         workbook.close()
        
# #     except Exception as e:
# #         logger.error(f"Error deleting sheets: {str(e)}")
# #         raise HTTPException(status_code=500, detail=f"Failed to delete sheets: {str(e)}")

# def delete_other_sheets(file_path, account_name):
#     """
#     Rename the first sheet to 'Apollo' and delete all other sheets.
#     """
#     try:
#         workbook = load_workbook(file_path)
        
#         # Step 1: Rename the first sheet to 'Apollo'
#         original_first_sheet = workbook.sheetnames[0]
#         workbook[original_first_sheet].title = 'Apollo'
#         apollo_sheet_name = 'Apollo'
#         logger.info(f"Renamed first sheet '{original_first_sheet}' to 'Apollo'")
        
#         # Step 2: Delete all other sheets except 'Apollo'
#         sheets_to_delete = [sheet for sheet in workbook.sheetnames if sheet != apollo_sheet_name]
#         for sheet_name in sheets_to_delete:
#             del workbook[sheet_name]
#             logger.info(f"Deleted sheet: {sheet_name}")
        
#         # Step 3: Save and close the workbook
#         workbook.save(file_path)
#         workbook.close()
    
#     except Exception as e:
#         logger.error(f"Error deleting sheets: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Failed to delete sheets: {str(e)}")


# def run_contact_processor_script(account_id):
#     """Run the contact processor script in background"""
#     def run_script():
#         try:
#             # Commands to run
#             commands = [
#                 r"call F:\qpilot_prod\qpilot_venv\Scripts\activate",
#                 r"cd /d F:\qpilot_prod\qpilot_v1_c023\DS\ai_framework\NextQSummary\scripts",
#                 f"python contact_processor.py --accountid {account_id}"
#             ]
            
#             # Combine commands with && to run in sequence
#             full_command = " && ".join(commands)
            
#             # Run the command in background
#             process = subprocess.Popen(full_command, shell=True, stdout=subprocess.PIPE, text=True, stderr=subprocess.STDOUT)
#             logger.info(f"Started contact processor script for account_id: {account_id}")
#             for line in process.stdout:
#                 print(line, end='')
            
#         except Exception as e:
#             logger.error(f"Error running contact processor script: {str(e)}")
    
#     # Run in background thread
#     thread = threading.Thread(target=run_script)
#     thread.daemon = True
#     thread.start()

# @app.post("/api/modify_contacts", dependencies=[Depends(verify_token)])
# async def api_modify_contacts(
#     file: UploadFile = File(...),
#     account: str = Form(...)
# ):
#     if not hasattr(app.state, 'ds_root'):
#         state = load_state()
#         if not state.get("ds_root"):
#             logger.error("app.state.ds_root not set; call /api/setup first")
#             raise HTTPException(status_code=400, detail="Call `/api/setup` first")
#         app.state.ds_root = state["ds_root"]

#     ds_root_path = Path(app.state.ds_root)
#     outdir = ds_root_path / "ai_framework" / "NextQSummary" / "input" / "Coresignal" / "excel"
#     outdir.mkdir(exist_ok=True, parents=True)
#     dest = outdir / f"{account}.xlsx"

#     logger.debug(f"Modifying contacts for account: {account}, destination: {dest}")

#     contents = await file.read()  # Read bytes from uploaded file
#     buffer = BytesIO(contents)

#     try:
#         # Read the uploaded Apexon format file
#         if file.filename.endswith('.xlsx'):
#             apexon_df = pd.read_excel(buffer)
#         else:  # assume CSV
#             buffer.seek(0)
#             apexon_df = pd.read_csv(buffer)
#     except Exception as e:
#         logger.exception("Failed to read uploaded file")
#         raise HTTPException(status_code=400, detail=f"Failed to read file: {str(e)}")

#     # Get account_id from database
#     account_id = get_account_id_from_db(account)
#     if not account_id:
#         logger.error(f"Could not find account_id for account: {account}")
#         raise HTTPException(status_code=400, detail=f"Account ID not found for: {account}")

#     # Convert Apexon format to Shell format
#     shell_df = convert_apexon_to_shell_format(apexon_df, account_id, account)

#     file_updated = False
    
#     if dest.exists():
#         try:
#             # Read existing file and append new data
#             existing_df = pd.read_excel(dest)
            
#             # Ensure both dataframes have the same columns
#             if set(existing_df.columns) == set(shell_df.columns):
#                 combined_df = pd.concat([existing_df, shell_df], ignore_index=True)
                
#                 # Remove duplicates based on email (assuming email is unique identifier)
#                 combined_df = combined_df.drop_duplicates(subset=['email'], keep='last')
                
#                 combined_df.to_excel(dest, index=False)
#                 logger.info(f"Appended to existing file: {dest}")
#                 file_updated = True
#             else:
#                 logger.warning("Column mismatch, replacing entire file")
#                 shell_df.to_excel(dest, index=False)
#                 logger.info(f"Replaced existing file: {dest}")
#                 file_updated = True
                
#         except Exception as e:
#             logger.exception("Failed to modify existing file")
#             raise HTTPException(status_code=500, detail=f"Failed to modify file: {str(e)}")
#     else:
#         shell_df.to_excel(dest, index=False)
#         logger.info(f"Created new file: {dest}")
#         file_updated = True

#     if file_updated:
#         try:
#             # Delete other sheets except Apollo
#             delete_other_sheets(dest, account)
            
#             # Create contact.json and comment out process_keycontact
#             create_contact_json(ds_root_path)
#             comment_out_process_keycontact(ds_root_path)
            
#             # Run contact processor script in background
#             run_contact_processor_script(account_id)
            
#             logger.info(f"Successfully processed contacts for account: {account}")
            
#         except Exception as e:
#             logger.error(f"Error in post-processing: {str(e)}")
#             # Don't raise exception here as the main upload was successful
            
#     return {
#         "filename": f"{account}.xlsx",
#         "message": "Contacts uploaded successfully. Processing started in background.",
#         "account_id": account_id
#     }

@app.post("/api/update_ranks", dependencies=[Depends(verify_token)])
async def api_update_ranks(payload: dict):
    if not hasattr(app.state, 'ds_root'):
        state = load_state()
        if not state.get("ds_root"):
            logger.error("app.state.ds_root not set; call /api/setup first")
            raise HTTPException(status_code=400, detail="Call `/api/setup` first")
        app.state.ds_root = state["ds_root"]
    account = payload.get("account")
    rows = payload.get("rows")
    if not account or not rows:
        logger.error("`account` and `rows` required")
        raise HTTPException(status_code=400, detail="`account` and `rows` required")
    for r in rows:
        r["accountname"] = account
    updated = update_ranks(rows)
    logger.info(f"Ranks updated for account: {account}")
    return {"updated": updated}

@app.get("/api/download_products_excel", dependencies=[Depends(verify_token)])
async def download_products_excel():
    if not hasattr(app.state, 'ds_root') or not hasattr(app.state, 'db_name'):
        state = load_state()
        if not state.get("ds_root") or not state.get("db_name"):
            logger.error("app.state.ds_root or app.state.db_name not set; call /api/setup first")
            raise HTTPException(status_code=400, detail="Call `/api/setup` first to configure paths and DB")
        app.state.ds_root = state["ds_root"]
        app.state.db_name = state["db_name"]
    sections_path = Path(app.state.ds_root) / "sections.json"
    logger.debug(f"Checking sections.json at: {sections_path}")
    if not sections_path.is_file():
        logger.error("sections.json file not found")
        raise HTTPException(status_code=500, detail="Missing sections.json file")
    try:
        with open(sections_path, "r") as f:
            data = json.load(f)
        customer_name = data[0].get("customer_name")
        customer_id = data[0].get("customer_id")
        logger.debug(f"Retrieved customer_name: {customer_name}, customer_id: {customer_id}")
        if not customer_name or not customer_id:
            logger.error("Missing customer_name or customer_id in sections.json")
            raise HTTPException(status_code=500, detail="Missing customer_name or customer_id in sections.json")
        query = """
            SELECT product 
            FROM dwh.f_genai_extracted_solvedchallenges 
            WHERE product IS NOT NULL AND product <> 'No Data';
        """
        conn = get_db_conn()
        df = pd.read_sql(query, conn)
        df = df.drop_duplicates().dropna()
        df.columns = ["Product"]
        conn.close()
        logger.info(f"Queried {len(df)} products from database")
        filename = f"{customer_name}_{customer_id}_offerings_product.xlsx"
        output_path = Path(app.state.ds_root) / filename
        df.to_excel(output_path, index=False)
        logger.info(f"Saved Excel file to: {output_path}")
        return FileResponse(
            path=output_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        logger.exception("Failed to generate offerings product Excel")
        raise HTTPException(status_code=500, detail="Failed to generate Excel file")

@app.post("/api/validate_path", dependencies=[Depends(verify_token)])
async def validate_ds_path(customer_id: str = Form(...)):
    ds_root = Path(f"F:/qpilot_prod/qpilot_v1_{customer_id}/DS")

    if not ds_root.exists():
        logger.error(f"Path does not exist: {ds_root}")
        raise HTTPException(status_code=404, detail=f"DS path not found: {ds_root}")

    sections_path = ds_root / "sections.json"
    if not sections_path.is_file():
        logger.error(f"sections.json missing at: {sections_path}")
        raise HTTPException(status_code=404, detail="sections.json not found")

    try:
        with open(sections_path, "r") as f:
            data = json.load(f)
        customer_name = data[0].get("customer_name")
        if not customer_name:
            logger.error("customer_name not found in sections.json")
            raise HTTPException(status_code=500, detail="customer_name missing")
        return {
            "ds_root": str(ds_root),
            "customer_name": customer_name
        }
    except Exception as e:
        logger.exception("Failed to load sections.json")
        raise HTTPException(status_code=500, detail="Failed to load sections.json")


if __name__ == "__main__":
    logger.info("Starting FastAPI server on http://0.0.0.0:8008")
    import uvicorn
    uvicorn.run("csm_backend_server:app", host="0.0.0.0", port=8008, workers=1)