import os
import json
import requests
import streamlit as st
import pandas as pd
import logging
from dotenv import load_dotenv

# --- Configuration ---
# Configure logging to provide insights into the application's behavior
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables for sensitive data like API keys
load_dotenv() # This will load variables from your .env file

# API Configuration - Centralized for easy management
API_BASE = os.getenv("API_BASE")
API_KEY = os.getenv("RM_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# --- Streamlit Page Setup ---
st.set_page_config(
    page_title="CSM Backend Portal - Next Quarter",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom Styling ---
# Apply custom CSS for a consistent and branded look and feel
# st.markdown("""
# <style>
#     /* General App Styling */
#     .stApp {
#         background-color: white;
#         color: black;
#     }
#     /* Button Styling */
#     .stButton>button {
#         background-color: #007BFF; /* A more modern blue */
#         color: white;
#         border-radius: 8px;
#         border: 1px solid #007BFF;
#         transition: background-color 0.3s ease, border-color 0.3s ease;
#     }
#     .stButton>button:hover {
#         background-color: #0056b3;
#         border-color: #0056b3;
#     }
#     /* Input Widget Styling */
#     .stSelectbox, .stTextInput, .stNumberInput, .stFileUploader {
#         background-color: #f8f9fa; /* A light grey for contrast */
#         border-radius: 8px;
#     }
#     /* Tab Styling */
#     .stTabs [data-baseweb="tab-list"] {
#         gap: 24px;
#     }
#     .stTabs [data-baseweb="tab"] {
#         height: 50px;
#         white-space: pre-wrap;
#         background-color: transparent;
#         border-radius: 8px;
#         border: 1px solid #E0E0E0;
#         transition: background-color 0.3s ease; /* Smooth transition for hover */
#     }
#     .stTabs [data-baseweb="tab"]:hover {
#         background-color: #f0f2f6; /* Light grey highlight on hover */
#     }
#     .stTabs [aria-selected="true"] {
#         background-color: #E0F7FA;
#         border-color: #007BFF;
#     }
# </style>
# """, unsafe_allow_html=True)
st.markdown("""
<style>
/* --- GLOBAL APP COLORS (unchanged) --- */
.stApp { background-color: white; color: black; }

/* --- BUTTONS & INPUTS (unchanged) --- */
.stButton>button {
    background-color: #007BFF;
    color: white;
    border-radius: 8px;
    border: 1px solid #007BFF;
    transition: background-color 0.3s ease, border-color 0.3s ease;
}
.stButton>button:hover { background-color: #0056b3; border-color: #0056b3; }
.stSelectbox, .stTextInput, .stNumberInput, .stFileUploader {
    background-color: #f8f9fa;
    border-radius: 8px;
}

/* --- TAB LIST --- */
.stTabs [data-baseweb="tab-list"] {
    gap: 16px;               /* smaller gap                 */
    border-bottom: 1px solid #ececec;  /* faint baseline */
    padding-bottom: 2px;
}

/* --- INDIVIDUAL TABS --- */
.stTabs [data-baseweb="tab"] {
    background: transparent; /* no box                       */
    border: none;            /* remove border                */
    padding: 0 4px;
    height: 42px;
    color: #555;
    font-weight: 500;
    border-bottom: 2px solid transparent; /* invisible underline */
    transition: color 0.2s ease, border-color 0.2s ease;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #000;
    border-bottom: 2px solid #ddd;     /* subtle hover line     */
}

/* --- ACTIVE TAB --- */
.stTabs [aria-selected="true"] {
    color: #29d40f;                      /* accent color         */
    border-bottom: 2px solid #29d40f;    /* accent underline     */
    background: transparent;             /* keep background clear*/
}
</style>
""", unsafe_allow_html=True)



# --- Session State Initialization ---
# Centralized initialization to ensure all required keys are present
def initialize_session_state():
    """Initializes all required keys in Streamlit's session state."""
    defaults = {
        'setup_complete': False,
        'ds_root': '',
        'customer_id': '',
        'customer_name': '',
        'account_names': [],
        'manual_rows': []
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

initialize_session_state()


# --- API Helper Functions ---
def make_api_request(method, endpoint, **kwargs):
    """A robust wrapper for making API requests with error handling."""
    url = f"{API_BASE}/api/{endpoint}"
    try:
        resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        st.error(f"HTTP Error: {e.response.status_code} - {e.response.text}")
        logger.error(f"HTTP Error for {url}: {e}")
    except requests.exceptions.RequestException as e:
        st.error(f"API Request Failed: {e}")
        logger.error(f"API request failed for {url}: {e}")
    return None

# --- UI Modules (Tabs) ---

def setup_tab():
    """Renders the UI for the initial setup process."""
    st.header("Step 1: Initial Setup")
    st.info("Please provide the Customer ID and select an operation to begin. This will populate the other tabs with customer-specific data.")

    with st.form("setup_form"):
        customer_id = st.text_input("Enter Customer ID", help="The unique identifier for the customer.", value=st.session_state.get('customer_id', ''))
        operation = st.selectbox("Select Operation",
                                 ["generate till initiatives", "generate only initiatives", "generate ranking", "generate reports", "Nothing"],
                                 help="Choose the primary operation you want to perform for this customer.")
        submitted = st.form_submit_button("Run Setup")

    if submitted:
        if not customer_id or not operation:
            st.error("Please enter a Customer ID and select an operation.")
            logger.warning("Setup attempted without Customer ID or operation.")
            return

        # with st.spinner("Validating path and fetching customer data..."):
        #     ds_root = f"F:/qpilot_prod/qpilot_v1_{customer_id}/DS"
        #     if not os.path.exists(ds_root):
        #         st.error(f"Repository path does not exist: {ds_root}")
        #         logger.error(f"Path validation failed: {ds_root}")
        #         return

        #     sections_path = os.path.join(ds_root, "sections.json")
        #     if not os.path.exists(sections_path):
        #         st.error("sections.json not found in the specified DS_ROOT.")
        #         logger.error(f"sections.json missing at {sections_path}")
        #         return

        #     with open(sections_path, "r") as f:
        #         sections_data = json.load(f)
        #     customer_name = sections_data[0].get("customer_name")

        #     if not customer_name:
        #         st.error("customer_name not found in sections.json.")
        #         logger.error("customer_name missing in sections.json")
        #         return
        with st.spinner("Validating path and fetching customer data..."):
            validate_resp = make_api_request("post", "validate_path", data={"customer_id": customer_id})
            if not validate_resp:
                return

            ds_root = validate_resp["ds_root"]
            customer_name = validate_resp["customer_name"]
        
            account_response = make_api_request("post", "accountnames", data={"customer_id": customer_id})
            if not account_response or not account_response.get("accounts"):
                st.error("No accounts found for this customer ID or failed to fetch them.")
                logger.warning(f"No accounts found for customer_id={customer_id}")
                return

            account_names = account_response["accounts"]
            selected_account = account_names[0]  # Default to the first account

            setup_data = {
                "ds_path": ds_root,
                "operation": operation,
                "account": selected_account
            }
            setup_response = make_api_request("post", "setup", data=setup_data)

            if setup_response:
                st.session_state['ds_root'] = ds_root
                st.session_state['customer_id'] = customer_id
                st.session_state['customer_name'] = customer_name
                st.session_state['account_names'] = account_names
                st.session_state['setup_complete'] = True
                st.success("Setup completed successfully! All tabs are now active.")
                logger.info(f"Setup completed: Customer ID={customer_id}, Operation={operation}, Account={selected_account}")
                st.rerun()


def feedback_tab():
    """Renders the UI for uploading feedback documents."""
    st.header("Upload Feedback Document")
    is_disabled = not st.session_state.setup_complete

    if is_disabled:
        st.info("Please complete the 'Initial Setup' tab first to enable this section.")

    uploaded_file = st.file_uploader(
        "Choose a .docx or .doc file",
        type=["docx", "doc"],
        help="Upload customer feedback documents here.",
        disabled=is_disabled
    )
    if uploaded_file:
        if st.button("Submit Feedback File", disabled=is_disabled):
            with st.spinner("Uploading and processing file..."):
                files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
                response = make_api_request("post", "upload", files=files)
                if response:
                    st.success(f"File uploaded successfully: {response.get('filename')}")
                    logger.info(f"Feedback uploaded: {uploaded_file.name}")


def ranks_tab():
    """Renders the UI for updating initiative ranks."""
    st.header("Update Initiative Ranks")
    is_disabled = not st.session_state.setup_complete

    if is_disabled:
        st.info("Please complete the 'Initial Setup' tab first to enable this section.")

    account = st.selectbox("Select Account Name", st.session_state.get('account_names', []), key="ranks_account", disabled=is_disabled)
    option = st.radio("Choose update method:", ["Upload Excel file", "Manual entry"], horizontal=True, disabled=is_disabled)

    if option == "Upload Excel file":
        excel_file = st.file_uploader("Upload an Excel file with 'initiativename' and 'rank' columns.", type=["xlsx"], disabled=is_disabled)
        if excel_file:
            try:
                df = pd.read_excel(excel_file)
                if {"initiativename", "rank"}.issubset(df.columns):
                    st.write("File Preview:")
                    st.dataframe(df[["initiativename", "rank"]])
                    if st.button("Submit Ranks from Excel", disabled=is_disabled):
                        rows = df[["initiativename", "rank"]].dropna().to_dict("records")
                        payload = {"account": account, "rows": rows}
                        with st.spinner("Updating ranks..."):
                            response = make_api_request("post", "update_ranks", json=payload)
                            if response:
                                st.success(f"Ranks updated successfully: {response.get('updated')} records.")
                                logger.info(f"Ranks updated from Excel for account: {account}")
                else:
                    st.error("The uploaded Excel file must contain 'initiativename' and 'rank' columns.")
            except Exception as e:
                st.error(f"Failed to read or process Excel file: {e}")
                logger.error(f"Excel processing failed: {e}")

    elif option == "Manual entry":
        st.subheader("Enter Initiative Ranks Manually")
        for i, row in enumerate(st.session_state['manual_rows']):
            col1, col2, col3 = st.columns([4, 1, 1])
            row['initiativename'] = col1.text_input("Initiative Name", value=row["initiativename"], key=f"ini_{i}", disabled=is_disabled)
            row['rank'] = col2.number_input("Rank", min_value=1, value=row["rank"], key=f"rank_{i}", disabled=is_disabled)
            if col3.button("Remove", key=f"del_{i}", disabled=is_disabled):
                st.session_state['manual_rows'].pop(i)
                st.rerun()

        if st.button("Add New Initiative", disabled=is_disabled):
            st.session_state['manual_rows'].append({"initiativename": "", "rank": len(st.session_state['manual_rows']) + 1})
            st.rerun()

        if st.session_state['manual_rows'] and st.button("Submit Manual Ranks", disabled=is_disabled):
            payload = {"account": account, "rows": st.session_state['manual_rows']}
            with st.spinner("Updating ranks..."):
                response = make_api_request("post", "update_ranks", json=payload)
                if response:
                    st.success(f"Ranks updated successfully: {response.get('updated')} records.")
                    st.session_state['manual_rows'] = []
                    logger.info(f"Manual ranks updated for account: {account}")
                    st.rerun()

def contacts_tab():
    """Renders the UI for managing contacts."""
    st.header("Manage Contacts")
    is_disabled = not st.session_state.setup_complete

    if is_disabled:
        st.info("Please complete the 'Initial Setup' tab first to enable this section.")

    account = st.selectbox("Select Account Name", st.session_state.get('account_names', []), key="contact_account", disabled=is_disabled)

    st.subheader("Upload New Contacts")
    contact_file = st.file_uploader("Upload a csv file with new contacts.", type=["csv"], key="contact_upload", disabled=is_disabled)
    if contact_file:
        if st.button("Submit New Contacts", disabled=is_disabled):
            files = {"file": (f"{account}.csv", contact_file.getvalue())}
            data = {"account": account}
            with st.spinner("Uploading new contacts..."):
                response = make_api_request("post", "upload_contacts", files=files, data=data)
                if response:
                    st.success("New contacts uploaded successfully.")
                    logger.info(f"Contacts uploaded for account: {account}")


def offerings_tab():
    """Renders the UI for downloading product offerings."""
    st.header("Download Product Offerings")
    is_disabled = not st.session_state.setup_complete

    if is_disabled:
        st.info("Please complete the 'Initial Setup' tab first to enable downloads.")
    
    st.info("Click the button below to download the product offerings for the current customer as an Excel file.")
    
    button_label = f"Download Offerings for {st.session_state['customer_name']}" if not is_disabled else "Download Offerings"
    
    if st.button(button_label, disabled=is_disabled):
        with st.spinner("Generating and downloading file..."):
            url = f"{API_BASE}/api/download_products_excel"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=60)
                resp.raise_for_status()
                st.download_button(
                    label="Click here to download",
                    data=resp.content,
                    file_name=f"{st.session_state['customer_name']}_product_offerings.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                logger.info(f"Product offerings downloaded for {st.session_state['customer_name']}")
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to download product offerings: {e}")
                logger.error(f"Download failed: {e}")

# --- Main Application Logic ---
def main():
    """The main function that orchestrates the Streamlit app."""
    st.title("CSM Backend Portal - Next Quarter")

    if st.session_state.setup_complete:
        st.sidebar.header("Customer Details")
        st.sidebar.markdown(f"**Name:** {st.session_state['customer_name']}")
        st.sidebar.markdown(f"**ID:** {st.session_state['customer_id']}")

    # Define tabs
    tab_titles = [
        "Initial Setup",
        "Upload Feedback",
        "Update Ranks",
        "Manage Contacts",
        "Product Offerings"
    ]
    tab1, tab2, tab3, tab4, tab5 = st.tabs(tab_titles)

    with tab1:
        setup_tab()
    with tab2:
        feedback_tab()
    with tab3:
        ranks_tab()
    with tab4:
        contacts_tab()
    with tab5:
        offerings_tab()


if __name__ == "__main__":
    if not API_KEY:
        st.error("API_KEY is not set. Please configure it in your environment variables.")
        logger.critical("RM_API_KEY environment variable not found.")
    else:
        main()