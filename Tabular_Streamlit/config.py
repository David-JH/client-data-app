"""
Column definitions and constants for the Tabular Streamlit app.
"""

import streamlit as st

# ── Qualification levels (shared by Sensitivities and Blockers) ──────────────

QUALIFICATION_OPTIONS = ["None", "Low", "Medium", "High", "Dealbreaker"]

# Numeric mapping for JSON storage.
QUALIFICATION_TO_VALUE = {
    "None": 0.0,
    "Low": 0.25,
    "Medium": 0.5,
    "High": 0.75,
    "Dealbreaker": 1.0,
}
VALUE_TO_QUALIFICATION = {v: k for k, v in reversed(list(QUALIFICATION_TO_VALUE.items()))}

# ── Sensitivity and Blocker types ────────────────────────────────────────────

SENSITIVITY_TYPES = ["Fees", "Friction", "Liquidity", "Margin", "Settlement"]

BLOCKER_TYPES = [
    "Habit (e.g. ICE Default)",
    "Systems Setup (EEX)",
    "Systems Setup (Client or External)",
    "Compliance",
    "Risk",
    "Onboarding/KYC",
    "Execution speed",
]

# ── Query to fetch data from the view ────────────────────────────────────────

VIEW_QUERY = """
    SELECT COMPANY, CLIENT_TYPE, CLIENT_STATUS, EEX_KAM, INCUBEX_KAM,
           SENSITIVITIES, BARRIERS,
           DECISION_MAKERS, EUA_VOLUME, GO_VOLUME, OTHER_PRODUCT_NOTES,
           ACCESS_TYPE, FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS,
           ETRM, SOURCE, NOTES, ENTRY_DATE, FIELD_UPDATE_DATES
    FROM STREAMLIT_APP_VIEW2
    ORDER BY COMPANY, CLIENT_TYPE
"""

VIEW_COLUMNS = [
    "COMPANY", "CLIENT_TYPE", "CLIENT_STATUS", "EEX_KAM", "INCUBEX_KAM",
    "SENSITIVITIES", "BARRIERS",
    "DECISION_MAKERS", "EUA_VOLUME", "GO_VOLUME", "OTHER_PRODUCT_NOTES",
    "ACCESS_TYPE", "FRONT_END", "FRONT_END_DETAILS", "CLEARERS", "BROKERS",
    "ETRM", "SOURCE", "NOTES", "ENTRY_DATE", "FIELD_UPDATE_DATES",
]

# ── Column classifications ───────────────────────────────────────────────────

KEY_COLUMNS = ["COMPANY", "CLIENT_TYPE"]

# Columns disabled in the data editor (not inline-editable).
# Note: EEX_KAM and INCUBEX_KAM are NOT read-only — they are editable so
# users can assign KAMs to prospects (written to PROSPECT_KAM, not CLIENTS).
READ_ONLY_COLUMNS = ["COMPANY", "CLIENT_TYPE", "ENTRY_DATE", "SENSITIVITIES", "BARRIERS"]

# KAM columns — editable inline, but routed to PROSPECT_KAM table on submit
KAM_COLUMNS = ["EEX_KAM", "INCUBEX_KAM"]

# Columns editable inline in the data editor
EDITABLE_COLUMNS = [
    "CLIENT_STATUS", "DECISION_MAKERS",
    "EUA_VOLUME", "GO_VOLUME", "OTHER_PRODUCT_NOTES", "ACCESS_TYPE",
    "FRONT_END", "FRONT_END_DETAILS", "CLEARERS", "BROKERS",
    "ETRM", "SOURCE", "NOTES",
]

# Columns edited via the below-table panel (not inline)
PANEL_EDIT_COLUMNS = ["SENSITIVITIES", "BARRIERS"]

# ── Column preset views ─────────────────────────────────────────────────────

COLUMN_PRESETS = {
    "All": None,  # None = show all columns
    "Meeting Notes": [
        "COMPANY", "CLIENT_TYPE", "CLIENT_STATUS", "EEX_KAM", "INCUBEX_KAM",
        "DECISION_MAKERS", "SOURCE", "NOTES", "NOTES_EDIT", "ENTRY_DATE",
    ],
    "Sensitivities & Blockers": [
        "COMPANY", "CLIENT_TYPE", "CLIENT_STATUS",
        "SENSITIVITIES", "BARRIERS", "EDIT",
        "EUA_VOLUME", "GO_VOLUME",
    ],
    "Technical": [
        "COMPANY", "CLIENT_TYPE",
        "ACCESS_TYPE", "FRONT_END", "FRONT_END_DETAILS",
        "CLEARERS", "BROKERS", "ETRM",
    ],
    "Volumes & Products": [
        "COMPANY", "CLIENT_TYPE", "CLIENT_STATUS",
        "EUA_VOLUME", "GO_VOLUME", "OTHER_PRODUCT_NOTES",
    ],
}

# ── All fields for info panel (display label -> DB column) ───────────────────

ALL_FIELDS_LABELS = {
    "Company": "COMPANY",
    "Client Type": "CLIENT_TYPE",
    "Status": "CLIENT_STATUS",
    "EEX KAM": "EEX_KAM",
    "Incubex KAM": "INCUBEX_KAM",
    "Sensitivities": "SENSITIVITIES",
    "Blockers": "BARRIERS",
    "Decision Makers": "DECISION_MAKERS",
    "EUA Volume": "EUA_VOLUME",
    "GO Volume": "GO_VOLUME",
    "Other Products": "OTHER_PRODUCT_NOTES",
    "Access Type": "ACCESS_TYPE",
    "Front End": "FRONT_END",
    "Front End Details": "FRONT_END_DETAILS",
    "Clearers": "CLEARERS",
    "Brokers": "BROKERS",
    "ETRM": "ETRM",
    "Source": "SOURCE",
    "Notes": "NOTES",
}

# ── INSERT query ─────────────────────────────────────────────────────────────

INSERT_QUERY = """
    INSERT INTO CLIENTS (
        CLIENT_STATUS, CLIENT_TYPE, COMPANY, SENSITIVITIES, BARRIERS,
        DECISION_MAKERS, OVERALL_VOLUME, EUA_VOLUME, GO_VOLUME,
        POWER_VOLUME, GAS_VOLUME, OTHER_PRODUCT_NOTES, ACCESS_TYPE,
        FRONT_END, FRONT_END_DETAILS, CLEARERS, BROKERS, ETRM, SOURCE, NOTES
    )
    SELECT
        %(client_status)s, %(client_type)s, %(company)s,
        PARSE_JSON(%(sensitivities)s), PARSE_JSON(%(barriers)s),
        %(decision_makers)s, %(overall_volume)s, %(eua_volume)s, %(go_volume)s,
        %(power_volume)s, %(gas_volume)s, %(other_product_notes)s, %(access_type)s,
        %(front_end)s, %(front_end_details)s, %(clearers)s, %(brokers)s,
        %(etrm)s, %(source)s, %(notes)s
"""


# ── Data editor column config ────────────────────────────────────────────────

def get_column_config(broker_options=None, clearer_options=None):
    """Return the st.data_editor column_config dict.

    Fixed widths prevent columns from resizing when Sensitivities / Blockers
    display strings change during panel edits.

    Parameters
    ----------
    broker_options : list[str] | None
        Dropdown options for the Brokers column.
    clearer_options : list[str] | None
        Dropdown options for the Clearers column.
    """
    return {
        "EDIT": st.column_config.CheckboxColumn("Edit S/B", default=False, width=70),
        "INFO": st.column_config.CheckboxColumn("Last Update Dates", default=False, width=120),
        "NOTES_EDIT": st.column_config.CheckboxColumn("Edit Notes", default=False, width=85),
        "COMPANY": st.column_config.TextColumn("Company", width=160),
        "CLIENT_TYPE": st.column_config.TextColumn("Client Type", width=80),
        "ENTRY_DATE": st.column_config.DateColumn("Entry Date", width=110),
        "CLIENT_STATUS": st.column_config.SelectboxColumn(
            "Status",
            options=["Client", "Prospect", "Setting up"],
            required=False,
            width=75,
        ),
        "EEX_KAM": st.column_config.TextColumn("EEX KAM", width=120),
        "INCUBEX_KAM": st.column_config.TextColumn("Incubex KAM", width=120),
        "SENSITIVITIES": st.column_config.TextColumn("Sensitivities", width=250),
        "BARRIERS": st.column_config.TextColumn("Blockers", width=250),
        "EUA_VOLUME": st.column_config.NumberColumn("EUA Volume", min_value=0, format="localized", width=100),
        "GO_VOLUME": st.column_config.NumberColumn("GO Volume", min_value=0, format="localized", width=100),
        "DECISION_MAKERS": st.column_config.TextColumn("Decision Makers", width=150),
        "OTHER_PRODUCT_NOTES": st.column_config.TextColumn("Other Products", width=150),
        "ACCESS_TYPE": st.column_config.SelectboxColumn(
            "Access Type",
            options=["NCM", "GCM", "DMA", "API", "Sponsored Access", "Voice", "Other"],
            required=False,
            width=105,
        ),
        "FRONT_END": st.column_config.TextColumn("Front End", width=95),
        "FRONT_END_DETAILS": st.column_config.TextColumn("Front End Details", width=140),
        "CLEARERS": st.column_config.SelectboxColumn(
            "Clearers",
            options=clearer_options or [],
            required=False,
            width=120,
        ),
        "BROKERS": st.column_config.SelectboxColumn(
            "Brokers",
            options=broker_options or [],
            required=False,
            width=120,
        ),
        "ETRM": st.column_config.SelectboxColumn(
            "ETRM",
            options=[
                "Allegro", "Amphora", "Aspect",
                "Brady (Igloo, Powerdesk, Crisk...)",
                "Comcore", "Eka", "Endure", "Entrade", "Entrader", "Ignite",
                "Inatech", "Lancelot", "Molecule", "Openlink", "PCI", "PexaOS",
                "Triplepoint", "Vuepoint",
            ],
            required=False,
            width=130,
        ),
        "SOURCE": st.column_config.SelectboxColumn(
            "Source",
            options=["Meeting", "Estimate", "Call"],
            required=False,
            width=100,
        ),
        "NOTES": st.column_config.TextColumn("Notes", width=180),
    }
