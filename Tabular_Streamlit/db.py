"""
Snowflake connection, data fetching, and insert logic.
"""

import json
import time

import pandas as pd
import snowflake.connector
import streamlit as st

from config import VIEW_QUERY, VIEW_COLUMNS, INSERT_QUERY, VALUE_TO_QUALIFICATION


def get_snowflake_connection(schema="CLIENTS", max_retries=3):
    """
    Create Snowflake connection using Streamlit secrets with retry logic.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database="INCUBEX_DATA_LAKE",
                schema=schema,
            )
            return conn
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(1)

    st.error(f"Connection error after {max_retries} attempts: {str(last_error)}")
    return None


def _parse_json_column(value) -> dict:
    """Parse a Snowflake VARIANT/JSON value into a Python dict."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _json_to_display(raw: dict) -> str:
    """Convert a {type: numeric_value} dict to a human-readable string.

    E.g. {"Margin": 0.75, "Fees": 0.25} -> "Margin: High, Fees: Low"
    """
    if not raw:
        return ""
    parts = []
    for key, val in raw.items():
        if isinstance(val, str):
            label = val  # already a label
        elif isinstance(val, (int, float)):
            label = VALUE_TO_QUALIFICATION.get(float(val), str(val))
        else:
            label = str(val)
        parts.append(f"{key}: {label}")
    return ", ".join(parts)


@st.cache_data(ttl=300)
def fetch_view_data() -> pd.DataFrame:
    """
    Fetch all client data from STREAMLIT_APP_VIEW.
    Adds display strings and raw dicts for SENSITIVITIES and BARRIERS.
    """
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame(columns=VIEW_COLUMNS)

    try:
        cursor = conn.cursor()
        cursor.execute(VIEW_QUERY)
        df = pd.DataFrame(cursor.fetchall(), columns=VIEW_COLUMNS)
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return pd.DataFrame(columns=VIEW_COLUMNS)
    finally:
        conn.close()

    # Parse JSON columns into raw dicts (for edit panel pre-population)
    df["_SENSITIVITIES_RAW"] = df["SENSITIVITIES"].apply(_parse_json_column)
    df["_BARRIERS_RAW"] = df["BARRIERS"].apply(_parse_json_column)

    # Parse per-field update dates JSON into a raw dict (for info panel)
    df["_FIELD_UPDATE_DATES_RAW"] = df["FIELD_UPDATE_DATES"].apply(_parse_json_column)

    # Convert JSON columns to human-readable display strings
    df["SENSITIVITIES"] = df["_SENSITIVITIES_RAW"].apply(_json_to_display)
    df["BARRIERS"] = df["_BARRIERS_RAW"].apply(_json_to_display)

    # Coerce volume columns to numeric
    for col in ("EUA_VOLUME", "GO_VOLUME"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def insert_changed_rows(rows: list[dict]) -> tuple[int, list[str]]:
    """
    Insert changed records into the CLIENTS table.

    Uses individual execute() calls within a single connection because
    the INSERT ... SELECT syntax (needed for PARSE_JSON) is incompatible
    with executemany().

    Parameters
    ----------
    rows : list of dicts matching the INSERT_QUERY parameter names.

    Returns
    -------
    (success_count, error_messages)
    """
    if not rows:
        return 0, []

    conn = get_snowflake_connection()
    if conn is None:
        return 0, ["Could not connect to Snowflake."]

    errors = []
    success_count = 0
    try:
        cursor = conn.cursor()
        for row in rows:
            try:
                cursor.execute(INSERT_QUERY, row)
                success_count += 1
            except Exception as e:
                errors.append(f"Insert failed for {row.get('company', '?')}: {e}")
        conn.commit()
        return success_count, errors
    except Exception as e:
        errors.append(f"Insert failed: {e}")
        return success_count, errors
    finally:
        conn.close()
