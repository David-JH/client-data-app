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
    </style>
""", unsafe_allow_html=True)


def get_snowflake_connection(schema="CLIENTS"):
    """
    Create Snowflake connection using Streamlit secrets.
    For Streamlit Cloud deployment, secrets are stored in .streamlit/secrets.toml
    """
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
        st.error(f"Connection error: {str(e)}")
        return None


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_company_list():
    """
    Fetch list of company names from COMPANY_NAMES view for autocomplete.
    """
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="INCUBEX_DATA_LAKE",
            schema="CLIENTS"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COMPANY_NAME FROM COMPANY_NAMES ORDER BY COMPANY_NAME")
        companies = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        conn.close()
        return companies
    except Exception as e:
        st.error(f"Error fetching company list: {str(e)}")
        return []


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_broker_list():
    """
    Fetch list of broker company names from BROKER_NAMES view.
    """
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="INCUBEX_DATA_LAKE",
            schema="CLIENTS"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COMPANY_NAME FROM BROKER_NAMES ORDER BY COMPANY_NAME")
        brokers = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        conn.close()
        return brokers
    except Exception as e:
        st.error(f"Error fetching broker list: {str(e)}")
        return []


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_clearer_list():
    """
    Fetch list of clearers from Snowflake.
    """
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database="INCUBEX_DATA_LAKE",
            schema="CLIENTS"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT COMPANY_NAME FROM CLEARERS ORDER BY COMPANY_NAME")
        clearers = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        conn.close()
        return clearers
    except Exception as e:
        st.error(f"Error fetching clearer list: {str(e)}")
        return []


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
            CLIENT_TYPE, COMPANY, WHY_NOT_TRADING, BARRIERS,
            DECISION_MAKERS, OVERALL_VOLUME, EUA_VOLUME, GO_VOLUME,
            POWER_VOLUME, GAS_VOLUME, OTHER_PRODUCT_NOTES, ACCESS_TYPE,
            FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS, ETRM, SOURCE, NOTES
        ) VALUES (
            %(client_type)s, %(company)s, %(why_not_trading)s, %(barriers)s,
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
    st.markdown("---")

    # Initialize session state for company fields
    if 'company_selection' not in st.session_state:
        st.session_state.company_selection = "Select a company..."
    if 'new_company_name' not in st.session_state:
        st.session_state.new_company_name = ""

    # Check if we need to reset company fields (set by successful form submission)
    if st.session_state.get('reset_company_fields', False):
        st.session_state.company_selection = "Select a company..."
        st.session_state.new_company_name = ""
        st.session_state.reset_company_fields = False

    # Show success message and balloons after rerun
    if st.session_state.get('show_success', False):
        st.success("Client data submitted successfully!")
        st.balloons()
        st.session_state.show_success = False

    # Fetch company list for autocomplete (outside form for caching)
    company_list = get_company_list()

    # Fetch broker list from Snowflake
    broker_list = get_broker_list()

    # Fetch clearer list from Snowflake
    clearer_list = get_clearer_list()

    # Company selection outside form for dynamic behavior
    company_selection = st.selectbox(
        "Company *",
        options=["Select a company...", "-- Enter new company --"] + company_list,
        index=0,
        key="company_selection",
        help="Select from list or choose 'Enter new company' to add a new prospect"
    )

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

    # Create form
    with st.form("client_form", clear_on_submit=True):

        # Client Type selection
        client_type = st.selectbox(
            "Client Type *",
            options=["Customer", "Clearer", "Broker"],
            index=None,
            placeholder="Select client type...",
            help="Select the type of client"
        )

        st.markdown('<p class="sub-header">Trading Information</p>', unsafe_allow_html=True)

        # Trading Info
        col1, col2 = st.columns(2)

        with col1:
            why_not_trading = st.text_area(
                "Why Not Trading",
                placeholder="e.g., Margin too high",
                help="Reason if not trading"
            )

        with col2:
            barriers = st.text_area(
                "Barriers",
                placeholder="e.g., Clearing not setup",
                help="Barriers to trading"
            )

        # Decision Makers
        decision_makers = st.text_input(
            "Decision Makers",
            placeholder="e.g., Head of desk (John Smith)",
            help="Key decision makers"
        )

        st.markdown('<p class="sub-header">Volume Information</p>', unsafe_allow_html=True)

        # Volumes - Stacked Vertically
        overall_volume = st.number_input(
            "Overall Volume (Lots)",
            min_value=0,
            value=None,
            help="Total trading volume"
        )

        eua_volume = st.number_input(
            "EUA Volume (Lots)",
            min_value=0,
            value=None,
            help="EU Allowance volume in lots (1,000 contracts)"
        )

        go_volume = st.number_input(
            "GO Volume (Lots)",
            min_value=0,
            value=None,
            help="Guarantees of Origin volume"
        )

        power_volume = st.number_input(
            "Power Volume (MWh)",
            min_value=0,
            value=None,
            help="Power trading volume"
        )

        gas_volume = st.number_input(
            "Gas Volume (MW)",
            min_value=0,
            value=None,
            help="Gas trading volume"
        )

        # Other Products
        other_product_notes = st.text_area(
            "Other Product Notes",
            placeholder="e.g., CBAM index interest",
            help="Notes on other products e.g. UKETS, CBAM, US products"
        )

        st.markdown('<p class="sub-header">Access & Systems</p>', unsafe_allow_html=True)

        # Access Info
        col1, col2 = st.columns(2)

        with col1:
            access_type = st.selectbox(
                "Access Type",
                options=["NCM", "GCM", "DMA", "API", "Sponsored Access", "Voice", "Other"],
                index=None,
                placeholder="e.g. NCM",
                help="Type of market access"
            )

        with col2:
            etrm = st.selectbox(
                "ETRM",
                options=[
                    "Allegro",
                    "Amphora",
                    "Aspect",
                    "Brady (Igloo, Powerdesk, Crisk...)",
                    "Comcore",
                    "Eka",
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
                ],
                index=None,
                placeholder="Select ETRM system",
                help="Energy Trading Risk Management system"
            )


        # Front End Info
        col1, col2 = st.columns(2)

        with col1:
            front_end = st.multiselect(
                "Front End",
                options=["TT", "Trayport", "Touchpoint", "Manual Entry", "CQG"],
                help="Trading front-end systems (select multiple)"
            )

        with col2:
            front_end_details = st.text_input(
                "Front End Details",
                placeholder="Additional details...",
                help="Additional front-end details"
            )

        # Service Providers
        col1, col2 = st.columns(2)

        with col1:
            clearers = st.multiselect(
                "Clearers",
                options=clearer_list,
                help="Clearing firms used (select multiple)"
            )

        with col2:
            brokers = st.multiselect(
                "Brokers",
                options=broker_list,
                help="Brokers used (select multiple)"
            )


        # Source
        source = st.selectbox(
            "Source",
            options=["Meeting", "Estimate", "Call"],
            index=None,
            placeholder="e.g. Meeting",
            help="Data source"
        )

        # Notes
        notes = st.text_area(
            "Notes",
            placeholder="Additional context and notes",
            help="Any additional information about the client"
        )

        st.markdown("---")

        # Submit button
        submitted = st.form_submit_button("Submit")

        if submitted:
            # Validation
            if not client_type:
                st.error("Client Type is required!")
            elif not company or company == "Select a company..." or company == "-- Enter new company --":
                st.error("Company name is required!")
            else:
                # Convert multi-select lists to comma-separated strings
                front_end_str = ", ".join(front_end) if front_end else None
                brokers_str = ", ".join(brokers) if brokers else None
                clearers_str = ", ".join(clearers) if clearers else None

                # Prepare data (no update_id or date - auto-generated in Snowflake)
                data = {
                    'client_type': client_type,
                    'company': company,
                    'why_not_trading': why_not_trading if why_not_trading else None,
                    'barriers': barriers if barriers else None,
                    'decision_makers': decision_makers if decision_makers else None,
                    'overall_volume': overall_volume,
                    'eua_volume': eua_volume,
                    'go_volume': go_volume,
                    'power_volume': power_volume,
                    'gas_volume': gas_volume,
                    'other_product_notes': other_product_notes if other_product_notes else None,
                    'access_type': access_type if access_type else None,
                    'front_end': front_end_str,
                    'front_end_details': front_end_details if front_end_details else None,
                    'clearers': clearers_str,
                    'brokers': brokers_str,
                    'etrm': etrm if etrm else None,
                    'source': source if source else None,
                    'notes': notes if notes else None
                }

                # Insert data
                if insert_client_data(data):
                    # Flag to reset company fields and show success on next rerun
                    st.session_state.reset_company_fields = True
                    st.session_state.show_success = True
                    st.rerun()
                else:
                    st.error("Failed to submit data. Please check your connection and try again.")

    # Show recent entries (optional)
    st.markdown("---")
    with st.expander("View Recent Entries"):
        conn = get_snowflake_connection()
        if conn:
            try:
                cursor = conn.cursor()
                query = "SELECT * FROM CLIENTS ORDER BY ENTRY_DATE DESC LIMIT 10"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                cursor.close()
                st.dataframe(df, use_container_width=True)
                conn.close()
            except Exception as e:
                st.info("No data available or unable to fetch recent entries.")


if __name__ == "__main__":
    main()
