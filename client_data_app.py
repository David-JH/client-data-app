"""
Streamlit App for Client Data Entry to Snowflake
Database: INCUBEX_DATA_LAKE / CLIENTS / CLIENTS
"""

import streamlit as st
import snowflake.connector
from datetime import date, datetime
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Client Data Entry",
    page_icon="📊",
    layout="centered"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1E3A5F;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #2E4A6F;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
    }
    /* Submit button styling */
    div.stFormSubmitButton {
        text-align: center !important;
    }
    div.stFormSubmitButton > button {
        background-color: #4CAF50 !important;
        color: white !important;
        font-weight: bold !important;
        min-width: 300px !important;
        width: 50% !important;
        border-radius: 8px !important;
        padding: 0.75rem 2rem !important;
        white-space: nowrap !important;
    }
    div.stFormSubmitButton > button:hover {
        background-color: #45a049 !important;
    }
    /* Client Type toggle button styling */
    div[data-testid="stHorizontalBlock"] .stButton > button {
        width: 100%;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        transition: all 0.2s ease;
    }
    .client-type-label {
        font-size: 1rem;
        font-weight: 600;
        color: #2E4A6F;
        margin-bottom: 0.5rem;
    }
    /* Light pink background for required fields (Company and Client Type) */
    .required-field div[data-baseweb="select"] > div {
        background-color: #fff5f5 !important;
    }
    </style>
""", unsafe_allow_html=True)


def get_snowflake_connection(schema="CLIENTS", max_retries=3):
    """
    Create Snowflake connection using Streamlit secrets with retry logic.
    For Streamlit Cloud deployment, secrets are stored in .streamlit/secrets.toml
    """
    import time

    last_error = None
    for attempt in range(max_retries):
        try:
            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database="INCUBEX_DATA_LAKE",
                schema=schema
            )
            return conn
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait 1 second before retrying

    st.error(f"Connection error after {max_retries} attempts: {str(last_error)}")
    return None


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_all_data():
    """
    Fetch all dropdown lists and client data using a single connection.
    Returns a tuple of (companies, brokers, clearers, clients_df, recent_df).
    """
    companies = []
    brokers = []
    clearers = []
    clients_df = pd.DataFrame()
    recent_df = pd.DataFrame()

    conn = get_snowflake_connection()
    if conn is None:
        return companies, brokers, clearers, clients_df, recent_df

    cursor = None
    try:
        cursor = conn.cursor()

        # Fetch all firm names (brokers, clearers, customers) in a single query
        cursor.execute("""
            SELECT DISTINCT BROKER, CLEARER, CUSTOMER
            FROM ALL_FIRM_NAMES
            WHERE BROKER IS NOT NULL OR CLEARER IS NOT NULL OR CUSTOMER IS NOT NULL
            ORDER BY BROKER, CLEARER, CUSTOMER
        """)
        for row in cursor.fetchall():
            if row[0]:  # BROKER column
                brokers.append(row[0])
            if row[1]:  # CLEARER column
                clearers.append(row[1])
            if row[2]:  # CUSTOMER column
                companies.append(row[2])

        # Fetch all client data from STREAMLIT_APP_VIEW view
        cursor.execute("""
            SELECT COMPANY, CLIENT_TYPE, CLIENT_STATUS, SENSITIVITIES, BARRIERS,
                   DECISION_MAKERS, EUA_VOLUME, GO_VOLUME, OTHER_PRODUCT_NOTES,
                   ACCESS_TYPE, FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS,
                   ETRM, SOURCE, NOTES
            FROM STREAMLIT_APP_VIEW
        """)
        columns = ['COMPANY', 'CLIENT_TYPE', 'CLIENT_STATUS', 'SENSITIVITIES', 'BARRIERS',
                   'DECISION_MAKERS', 'EUA_VOLUME', 'GO_VOLUME', 'OTHER_PRODUCT_NOTES',
                   'ACCESS_TYPE', 'FRONT_END', 'FRONT_END_DETAILS', 'CLEARERS', 'BROKERS',
                   'ETRM', 'SOURCE', 'NOTES']
        clients_df = pd.DataFrame(cursor.fetchall(), columns=columns)

        # Fetch 5 most recent records for display
        cursor.execute("""
            SELECT ENTRY_DATE, COMPANY, CLIENT_TYPE, CLIENT_STATUS, SENSITIVITIES, BARRIERS,
                   DECISION_MAKERS, EUA_VOLUME, GO_VOLUME, OTHER_PRODUCT_NOTES,
                   ACCESS_TYPE, FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS,
                   ETRM, SOURCE, NOTES
            FROM STREAMLIT_APP_VIEW
            ORDER BY ENTRY_DATE DESC
            LIMIT 5
        """)
        recent_columns = ['ENTRY_DATE', 'COMPANY', 'CLIENT_TYPE', 'CLIENT_STATUS', 'SENSITIVITIES', 'BARRIERS',
                          'DECISION_MAKERS', 'EUA_VOLUME', 'GO_VOLUME', 'OTHER_PRODUCT_NOTES',
                          'ACCESS_TYPE', 'FRONT_END', 'FRONT_END_DETAILS', 'CLEARERS', 'BROKERS',
                          'ETRM', 'SOURCE', 'NOTES']
        recent_df = pd.DataFrame(cursor.fetchall(), columns=recent_columns)

    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return companies, brokers, clearers, clients_df, recent_df


def parse_comma_string(value: str) -> list:
    """Parse a comma-separated string into a list of stripped values."""
    if not value or pd.isna(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def get_prefill_data(clients_df: pd.DataFrame, company: str, client_type: str) -> dict:
    """
    Get prefill data for a specific company and client type combination.
    Returns a dict with field values or empty dict if no match.
    """
    if clients_df.empty or not company or not client_type:
        return {}

    mask = (clients_df['COMPANY'] == company) & (clients_df['CLIENT_TYPE'] == client_type)
    matched = clients_df[mask]

    if matched.empty:
        return {}

    row = matched.iloc[0]
    return {
        'client_status': row['CLIENT_STATUS'] if pd.notna(row['CLIENT_STATUS']) else None,
        'sensitivities': parse_comma_string(row['SENSITIVITIES']),
        'barriers': parse_comma_string(row['BARRIERS']),
        'decision_makers': row['DECISION_MAKERS'] if pd.notna(row['DECISION_MAKERS']) else "",
        'eua_volume': int(float(row['EUA_VOLUME'])) if pd.notna(row['EUA_VOLUME']) else None,
        'go_volume': int(float(row['GO_VOLUME'])) if pd.notna(row['GO_VOLUME']) else None,
        'other_product_notes': row['OTHER_PRODUCT_NOTES'] if pd.notna(row['OTHER_PRODUCT_NOTES']) else "",
        'access_type': row['ACCESS_TYPE'] if pd.notna(row['ACCESS_TYPE']) else None,
        'front_end': parse_comma_string(row['FRONT_END']),
        'front_end_details': row['FRONT_END_DETAILS'] if pd.notna(row['FRONT_END_DETAILS']) else "",
        'clearers': parse_comma_string(row['CLEARERS']),
        'brokers': parse_comma_string(row['BROKERS']),
        'etrm': row['ETRM'] if pd.notna(row['ETRM']) else None,
        'source': row['SOURCE'] if pd.notna(row['SOURCE']) else None,
        'notes': row['NOTES'] if pd.notna(row['NOTES']) else "",
    }


def insert_client_data(data: dict) -> bool:
    """
    Insert client data into Snowflake CLIENTS table.
    UPDATE_ID and DATE are auto-generated by Snowflake.
    """
    conn = get_snowflake_connection()
    if conn is None:
        return False

    try:
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO CLIENTS (
            CLIENT_STATUS, CLIENT_TYPE, COMPANY, SENSITIVITIES, BARRIERS,
            DECISION_MAKERS, OVERALL_VOLUME, EUA_VOLUME, GO_VOLUME,
            POWER_VOLUME, GAS_VOLUME, OTHER_PRODUCT_NOTES, ACCESS_TYPE,
            FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS, ETRM, SOURCE, NOTES
        ) VALUES (
            %(client_status)s, %(client_type)s, %(company)s, %(sensitivities)s, %(barriers)s,
            %(decision_makers)s, %(overall_volume)s, %(eua_volume)s, %(go_volume)s,
            %(power_volume)s, %(gas_volume)s, %(other_product_notes)s, %(access_type)s,
            %(front_end)s, %(front_end_details)s, %(clearers)s, %(brokers)s, %(etrm)s, %(source)s, %(notes)s
        )
        """

        cursor.execute(insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()
        return True

    except Exception as e:
        st.error(f"Insert error: {str(e)}")
        if conn:
            conn.close()
        return False


def main():
    st.markdown('<p class="main-header">📊 Client Data Entry Form</p>', unsafe_allow_html=True)
    st.markdown("Enter client information below to add to the database.")

    st.info("""
**Required fields:** Company and Client Type (marked with *)

**Note:** Resubmitting for an existing company will update the record.
""")

    with st.expander("📖 User Guide"):
        st.markdown("""
**Getting Started**
- Select an existing company from the dropdown, or choose "Enter new company" to add a new prospect
- Choose the Client Type (Customer, Clearer, or Broker) - this is required
- If a record already exists for the Company + Client Type combination, the form will pre-fill with existing data

**Updating Records**
- To update an existing record, simply select the same Company and Client Type, make your changes, and submit
- The new submission will become the current record (previous versions are retained in history)

**Volume Information**
- You can enter volumes as an estimated range OR an exact number
- If you enter both, the exact number takes priority

**Service Providers**
- Select clearers and brokers from the dropdown lists
- If a clearer or broker isn't in the list, check "Add new" and type the name(s) comma-separated

**Refresh Data**
- After submitting, your new entry won't appear in dropdowns immediately (data is cached for performance)
- Click "Refresh Data" below the Submit button to reload the latest data from the database
""")

    st.markdown("---")

    # Initialize session state for company fields
    if 'company_selection' not in st.session_state:
        st.session_state.company_selection = "Select a company..."
    if 'new_company_name' not in st.session_state:
        st.session_state.new_company_name = ""
    if 'client_type_selection' not in st.session_state:
        st.session_state.client_type_selection = None

    # Initialize session state for clearers/brokers fields
    if 'clearers' not in st.session_state:
        st.session_state.clearers = []
    if 'add_new_clearer' not in st.session_state:
        st.session_state.add_new_clearer = False
    if 'additional_clearers' not in st.session_state:
        st.session_state.additional_clearers = ""
    if 'brokers' not in st.session_state:
        st.session_state.brokers = []
    if 'add_new_broker' not in st.session_state:
        st.session_state.add_new_broker = False
    if 'additional_brokers' not in st.session_state:
        st.session_state.additional_brokers = ""

    # Check if we need to reset fields (set by successful form submission)
    if st.session_state.get('reset_company_fields', False):
        st.session_state.company_selection = "Select a company..."
        st.session_state.new_company_name = ""
        st.session_state.client_type_selection = None
        st.session_state.clearers = []
        st.session_state.add_new_clearer = False
        st.session_state.additional_clearers = ""
        st.session_state.brokers = []
        st.session_state.add_new_broker = False
        st.session_state.additional_brokers = ""
        st.session_state.go_volume_range = None
        st.session_state.go_volume_exact = None
        st.session_state.previous_selection = None
        st.session_state.reset_company_fields = False

    # Show success message and balloons after rerun
    if st.session_state.get('show_success', False):
        st.success("Client data submitted successfully!")
        st.balloons()
        st.session_state.show_success = False

    # Fetch all data using single connection
    company_list, broker_list, clearer_list, clients_df, recent_df = get_all_data()

    # Company selection outside form for dynamic behavior
    st.markdown('<div class="required-field">', unsafe_allow_html=True)
    company_selection = st.selectbox(
        "Company *",
        options=["Select a company...", "-- Enter new company --"] + company_list,
        index=0,
        key="company_selection",
        help="Select from list or choose 'Enter new company' to add a new prospect"
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # Show text input if user wants to enter a new company
    if company_selection == "-- Enter new company --":
        company = st.text_input(
            "New Company Name *",
            placeholder="Enter company name...",
            key="new_company_name",
            help="Type the name of the new company/prospect"
        )
    else:
        company = company_selection

    # Client Type selection outside form for dynamic prefill
    st.markdown('<div class="required-field">', unsafe_allow_html=True)
    client_type = st.selectbox(
        "Client Type *",
        options=["Customer", "Clearer", "Broker"],
        index=None,
        placeholder="Select client type...",
        key="client_type_selection",
        help="Select the type of client"
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # Get prefill data based on Company + Client Type selection
    prefill = get_prefill_data(clients_df, company, client_type)

    # Track current selection to detect changes
    current_selection = f"{company}|{client_type}"
    previous_selection = st.session_state.get('previous_selection', None)

    # Update session state fields when selection changes
    if current_selection != previous_selection:
        st.session_state.previous_selection = current_selection
        if prefill:
            st.session_state.clearers = prefill.get('clearers', [])
            st.session_state.brokers = prefill.get('brokers', [])
            st.session_state.go_volume_range = None
            st.session_state.go_volume_exact = prefill.get('go_volume')
        else:
            # Clear if no prefill data for new selection
            st.session_state.clearers = []
            st.session_state.brokers = []
            st.session_state.go_volume_range = None
            st.session_state.go_volume_exact = None

    # Clearers and Brokers outside form for dynamic checkbox behavior
    st.markdown('<p class="sub-header">Service Providers</p>', unsafe_allow_html=True)

    # Extend dropdown options with any prefilled values not already in the list
    clearer_options = list(clearer_list)
    for c in prefill.get('clearers', []):
        if c and c not in clearer_options:
            clearer_options.append(c)

    broker_options = list(broker_list)
    for b in prefill.get('brokers', []):
        if b and b not in broker_options:
            broker_options.append(b)

    col1, col2 = st.columns(2)

    with col1:
        clearers = st.multiselect(
            "Clearers",
            options=clearer_options,
            key="clearers",
            help="Clearing firms used (select multiple)"
        )
        add_new_clearer = st.checkbox("Add new clearer not in list", key="add_new_clearer")
        if add_new_clearer:
            additional_clearers = st.text_input(
                "New Clearer(s)",
                placeholder="Enter new clearer names (comma-separated)",
                key="additional_clearers",
                help="Add clearers not in the list above"
            )
        else:
            additional_clearers = ""

    with col2:
        brokers = st.multiselect(
            "Brokers",
            options=broker_options,
            key="brokers",
            help="Brokers used (select multiple)"
        )
        add_new_broker = st.checkbox("Add new broker not in list", key="add_new_broker")
        if add_new_broker:
            additional_brokers = st.text_input(
                "New Broker(s)",
                placeholder="Enter new broker names (comma-separated)",
                key="additional_brokers",
                help="Add brokers not in the list above"
            )
        else:
            additional_brokers = ""

    # Create form
    with st.form("client_form", clear_on_submit=True):

        # Client Status - with prefill
        status_options = ["Client", "Prospect", "Setting up"]
        status_index = None
        if prefill.get('client_status') in status_options:
            status_index = status_options.index(prefill['client_status'])
        client_status = st.selectbox(
            "Client Status",
            options=status_options,
            index=status_index,
            placeholder="Select status...",
            help="Current status of the client"
        )

        st.markdown('<p class="sub-header">Trading Information</p>', unsafe_allow_html=True)

        # Trading Info
        col1, col2 = st.columns(2)

        with col1:
            sensitivities_options = ["Margin", "Fees", "Liquidity"]
            sensitivities = st.multiselect(
                "Sensitivities",
                options=sensitivities_options,
                default=[s for s in prefill.get('sensitivities', []) if s in sensitivities_options],
                help="Key issues that direct flow (select multiple)"
            )

        with col2:
            barriers_options = [
                "ICE Default",
                "Fees",
                "Margin",
                "Liquidity",
                "IT Setup (us)",
                "IT Setup (them)",
                "Compliance",
                "Risk",
                "Onboarding/KYC"
            ]
            barriers = st.multiselect(
                "Barriers",
                options=barriers_options,
                default=[b for b in prefill.get('barriers', []) if b in barriers_options],
                help="Barriers to trading (select multiple)"
            )

        # Decision Makers - with prefill
        decision_makers = st.text_input(
            "Decision Makers",
            value=prefill.get('decision_makers', ""),
            placeholder="e.g., Head of desk (John Smith)",
            help="Key decision makers"
        )

        st.markdown('<p class="sub-header">Volume Information</p>', unsafe_allow_html=True)
        st.caption("Client's total annual volumes across all exchanges. Enter volume range or exact number per product.")

        # EUA Volume - Range dropdown OR exact number input (prefill to exact field)
        st.markdown("EUA Volume (Lots)")
        eua_col1, eua_col2 = st.columns(2)
        with eua_col1:
            eua_volume_range = st.selectbox(
                "Estimated Range",
                options=[None, "<2.5k", "2.5-5k", "5-10k", "10-20k", "20-50k", "50k+"],
                index=0,
                format_func=lambda x: "Select range..." if x is None else x,
                help="Select an estimated volume range"
            )
        with eua_col2:
            eua_volume_exact = st.number_input(
                "Exact Volume",
                min_value=0,
                value=prefill.get('eua_volume'),
                help="Or enter exact volume in lots"
            )

        # GO Volume - Range dropdown OR exact number input (prefill to exact field)
        st.markdown("GO Volume (Lots)")
        go_col1, go_col2 = st.columns(2)
        with go_col1:
            go_volume_range = st.selectbox(
                "Estimated Range",
                options=[None, "<2.5k", "2.5-5k", "5-10k", "10-20k", "20k+"],
                format_func=lambda x: "Select range..." if x is None else x,
                help="Select an estimated volume range",
                key="go_volume_range"
            )
        with go_col2:
            go_volume_exact = st.number_input(
                "Exact Volume",
                min_value=0,
                help="Or enter exact volume in lots",
                key="go_volume_exact"
            )

        # Other Products - with prefill
        other_product_notes = st.text_area(
            "Other Product Notes",
            value=prefill.get('other_product_notes', ""),
            placeholder="e.g., CBAM index interest",
            help="Notes on other products e.g. UKETS, CBAM, US products"
        )

        st.markdown('<p class="sub-header">Access & Systems</p>', unsafe_allow_html=True)

        # Access Info - with prefill
        col1, col2 = st.columns(2)

        with col1:
            access_options = ["NCM", "GCM", "DMA", "API", "Sponsored Access", "Voice", "Other"]
            access_index = None
            if prefill.get('access_type') in access_options:
                access_index = access_options.index(prefill['access_type'])
            access_type = st.selectbox(
                "Access Type",
                options=access_options,
                index=access_index,
                placeholder="e.g. NCM",
                help="Type of market access"
            )

        with col2:
            etrm_options = [
                "Allegro",
                "Amphora",
                "Aspect",
                "Brady (Igloo, Powerdesk, Crisk...)",
                "Comcore",
                "Eka",
                "Endure",
                "Entrade",
                "Entrader",
                "Ignite",
                "Inatech",
                "Lancelot",
                "Molecule",
                "Openlink",
                "PCI",
                "PexaOS",
                "Triplepoint",
                "Vuepoint"
            ]
            etrm_index = None
            if prefill.get('etrm') in etrm_options:
                etrm_index = etrm_options.index(prefill['etrm'])
            etrm = st.selectbox(
                "ETRM",
                options=etrm_options,
                index=etrm_index,
                placeholder="Select ETRM system",
                help="Energy Trading Risk Management system"
            )


        # Front End Info - with prefill
        col1, col2 = st.columns(2)

        with col1:
            front_end_options = ["TT", "Trayport", "Touchpoint", "Manual Entry", "CQG"]
            front_end = st.multiselect(
                "Front End",
                options=front_end_options,
                default=[f for f in prefill.get('front_end', []) if f in front_end_options],
                help="Trading front-end systems (select multiple)"
            )

        with col2:
            front_end_details = st.text_input(
                "Front End Details",
                value=prefill.get('front_end_details', ""),
                placeholder="Additional details...",
                help="Additional front-end details"
            )

        # Source - with prefill
        source_options = ["Meeting", "Estimate", "Call"]
        source_index = None
        if prefill.get('source') in source_options:
            source_index = source_options.index(prefill['source'])
        source = st.selectbox(
            "Source",
            options=source_options,
            index=source_index,
            placeholder="e.g. Meeting",
            help="Data source"
        )

        # Notes - with prefill
        notes = st.text_area(
            "Notes",
            value=prefill.get('notes', ""),
            placeholder="Additional context and notes",
            help="Any additional information about the client"
        )

        st.markdown("---")

        # Submit button
        submitted = st.form_submit_button(label="**Submit**", width="stretch")

        if submitted:
            # Validation
            if not client_type:
                st.error("Client Type is required!")
            elif not company or company == "Select a company..." or company == "-- Enter new company --":
                st.error("Company name is required!")
            else:
                # Convert multi-select lists to comma-separated strings
                front_end_str = ", ".join(front_end) if front_end else None

                # Combine selected clearers with additional ones
                all_clearers = list(clearers) if clearers else []
                if additional_clearers:
                    all_clearers.extend([c.strip() for c in additional_clearers.split(",") if c.strip()])
                clearers_str = ", ".join(all_clearers) if all_clearers else None

                # Combine selected brokers with additional ones
                all_brokers = list(brokers) if brokers else []
                if additional_brokers:
                    all_brokers.extend([b.strip() for b in additional_brokers.split(",") if b.strip()])
                brokers_str = ", ".join(all_brokers) if all_brokers else None

                # Range to midpoint mapping
                range_midpoints = {
                    "<2.5k": 1250,      # midpoint of 0-2500
                    "2.5-5k": 3750,     # midpoint of 2500-5000
                    "5-10k": 7500,      # midpoint of 5000-10000
                    "10-20k": 15000,    # midpoint of 10000-20000
                    "20-50k": 35000,    # midpoint of 20000-50000
                    "50k+": 50000,      # representative value for 50k+
                    "20k+": 20000,      # legacy value for GO volume
                }

                # Process EUA volume - exact value takes priority, otherwise use range midpoint
                eua_volume = None
                if eua_volume_exact is not None and eua_volume_exact > 0:
                    eua_volume = eua_volume_exact
                    if eua_volume_range is not None:
                        st.info("Note: Using exact EUA volume value (range selection ignored)")
                elif eua_volume_range is not None:
                    eua_volume = range_midpoints.get(eua_volume_range)

                # Process GO volume - exact value takes priority, otherwise use range midpoint
                go_volume = None
                if go_volume_exact is not None and go_volume_exact > 0:
                    go_volume = go_volume_exact
                    if go_volume_range is not None:
                        st.info("Note: Using exact GO volume value (range selection ignored)")
                elif go_volume_range is not None:
                    go_volume = range_midpoints.get(go_volume_range)

                # Convert multiselect lists to comma-separated strings
                sensitivities_str = ", ".join(sensitivities) if sensitivities else None
                barriers_str = ", ".join(barriers) if barriers else None

                # Helper function to check if a value has changed from prefill
                def has_changed(field_name, new_value, is_list=False):
                    """Compare new value against prefill. Returns True if changed or no prefill exists."""
                    if not prefill:
                        # No prefill = new record, save everything
                        return True
                    prefill_value = prefill.get(field_name)
                    if is_list:
                        # Compare lists (order-independent)
                        prefill_list = prefill_value if prefill_value else []
                        new_list = new_value if new_value else []
                        return set(prefill_list) != set(new_list)
                    else:
                        # Compare scalar values (treat empty string as None)
                        if prefill_value == "" or prefill_value is None:
                            prefill_value = None
                        if new_value == "" or new_value is None:
                            new_value = None
                        return prefill_value != new_value

                # Prepare data - only include changed fields (always include company and client_type)
                data = {
                    'client_status': (client_status if client_status else None) if has_changed('client_status', client_status) else None,
                    'client_type': client_type,  # Always save
                    'company': company,  # Always save
                    'sensitivities': sensitivities_str if has_changed('sensitivities', sensitivities, is_list=True) else None,
                    'barriers': barriers_str if has_changed('barriers', barriers, is_list=True) else None,
                    'decision_makers': (decision_makers if decision_makers else None) if has_changed('decision_makers', decision_makers) else None,
                    'overall_volume': None,
                    'eua_volume': eua_volume if has_changed('eua_volume', eua_volume) else None,
                    'go_volume': go_volume if has_changed('go_volume', go_volume) else None,
                    'power_volume': None,
                    'gas_volume': None,
                    'other_product_notes': (other_product_notes if other_product_notes else None) if has_changed('other_product_notes', other_product_notes) else None,
                    'access_type': (access_type if access_type else None) if has_changed('access_type', access_type) else None,
                    'front_end': front_end_str if has_changed('front_end', front_end, is_list=True) else None,
                    'front_end_details': (front_end_details if front_end_details else None) if has_changed('front_end_details', front_end_details) else None,
                    'clearers': clearers_str if has_changed('clearers', all_clearers, is_list=True) else None,
                    'brokers': brokers_str if has_changed('brokers', all_brokers, is_list=True) else None,
                    'etrm': (etrm if etrm else None) if has_changed('etrm', etrm) else None,
                    'source': (source if source else None) if has_changed('source', source) else None,
                    'notes': (notes if notes else None) if has_changed('notes', notes) else None
                }

                # Insert data
                with st.spinner("Submitting data..."):
                    success = insert_client_data(data)

                if success:
                    # Flag to reset company fields and show success on next rerun
                    st.session_state.reset_company_fields = True
                    st.session_state.show_success = True
                    st.rerun()
                else:
                    st.error("Failed to submit data. Please check your connection and try again.")

    # Refresh Data button (outside form)
    st.markdown("")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Refresh Data", help="Refresh dropdown lists to see newly added entries", use_container_width=True):
            get_all_data.clear()
            st.rerun()

    # Display 5 most recent records
    st.markdown("---")
    st.markdown('<p class="sub-header">Recent Records</p>', unsafe_allow_html=True)

    if not recent_df.empty:
        st.dataframe(
            recent_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No recent records found.")


if __name__ == "__main__":
    main()
