"""
Tabular Client Data Editor
--------------------------
Displays all client data from STREAMLIT_APP_VIEW in an editable table.
Changes are submitted as new rows (append-only history) into the CLIENTS table.

Run from the project root so .streamlit/secrets.toml is picked up:
    streamlit run Tabular_Streamlit/tabular_app.py
"""

import json
from datetime import datetime

import pandas as pd
import streamlit as st

from config import (
    EDITABLE_COLUMNS,
    READ_ONLY_COLUMNS,
    SENSITIVITY_TYPES,
    BLOCKER_TYPES,
    QUALIFICATION_OPTIONS,
    QUALIFICATION_TO_VALUE,
    VALUE_TO_QUALIFICATION,
    ALL_FIELDS_LABELS,
    COLUMN_PRESETS,
    get_column_config,
)
from db import fetch_view_data, insert_changed_rows

# == Page config ===============================================================

st.set_page_config(page_title="Client Data - Tabular View", page_icon="📋", layout="wide")

st.markdown(
    """
    <style>
    .main-header {font-size: 2rem; font-weight: 700; color: #1E3A5F; margin-bottom: 0.5rem;}
    .sub-header  {font-size: 1.2rem; font-weight: 600; color: #2E4A6F;}
    div[data-testid="stDataEditor"] {border: 1px solid #ddd; border-radius: 8px;}
    /* Make data editor column headers bold and black */
    div[data-testid="stDataEditor"] [data-testid="glide-data-grid-canvas"] {
        --gdg-text-header: #000000 !important;
        --gdg-text-header-selected: #000000 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<p class="main-header">📋 Client Data - Tabular View</p>', unsafe_allow_html=True)

# -- User guide ----------------------------------------------------------------

with st.expander("How to use this page", expanded=False):
    st.markdown(
        """
**Searching**
- Use the **Search company** dropdown above the table to filter rows by company name.
  You can type in the dropdown to quickly find a company. Select **All** to show all rows.
- Use the **Search KAM** dropdown to filter by Key Account Manager. This searches across
  both EEX KAM and Incubex KAM columns. Both filters can be used together.

**Column presets**
- Use the preset buttons below the search bars to switch between different column views.
- **All** shows every column. **Meeting Notes** focuses on client details, notes, and source.
  **Sensitivities & Blockers** shows S/B data and volumes. **Technical** shows infrastructure details.
  **Volumes & Products** shows commercial data.
- The **Last Update Dates** column is always visible on the far right in all presets.

**Viewing data**
- The table below shows all client records from the database.
- You can **sort** any column by clicking its header, and **filter** by typing in the column filter row.
- Columns are scrollable horizontally if the table is wider than your screen.

**Editing inline fields**
- Most columns (Status, Decision Makers, Volumes, Access Type, etc.) can be edited
  directly in the table -- just click a cell and type or select a new value.

**Editing Sensitivities & Blockers**
- These columns contain structured data and cannot be edited inline.
- To edit them, tick the **Edit S/B** checkbox (the column after Blockers).
  This opens a panel below the table where you can set each sensitivity/blocker type
  and its qualification level (Unknown, None, Low, Medium, High, Dealbreaker).
- Changes are previewed immediately in the Sensitivities / Blockers columns.

**Viewing field update dates**
- Tick the **Last Update Dates** checkbox (far right of each row) to see when each field was last updated.
- Only one panel (Edit S/B or Last Update Dates) can be open at a time -- ticking one closes the other.

**Saving changes**
- When you are finished editing, click **Submit Changes**.
- Only rows with actual changes are saved (as new records in the database).
- Click **Refresh Data** to reload the latest data from Snowflake.

**Entry Date colours**
- 🟢 Green = updated within the last 14 days
- 🟡 Yellow = updated 15-60 days ago
- 🔴 Red = updated more than 60 days ago
        """
    )


# == Helpers ===================================================================

def _raw_dict_to_labels(raw) -> dict:
    """Convert {type: numeric} -> {type: label} for pre-populating dropdowns."""
    if not raw or not isinstance(raw, dict):
        return {}
    result = {}
    for key, val in raw.items():
        if isinstance(val, str):
            result[key] = val
        elif isinstance(val, (int, float)):
            result[key] = VALUE_TO_QUALIFICATION.get(float(val), QUALIFICATION_OPTIONS[0])
        else:
            result[key] = QUALIFICATION_OPTIONS[0]
    return result


def _labels_to_display(labels: dict) -> str:
    """Convert {type: label} dict to display string like 'Margin: High, Fees: Low'."""
    if not labels:
        return ""
    return ", ".join(f"{k}: {v}" for k, v in labels.items())


def _panel_edit_to_labels(pe: dict, field: str) -> dict:
    """Convert a panel_edits entry back to {type: label} for display.

    pe[field] is a dict like {"Margin": 0.75, "Fees": 0.25} (numeric values).
    """
    raw = pe.get(field, {})
    if not raw:
        return {}
    return {k: VALUE_TO_QUALIFICATION.get(float(v), str(v)) for k, v in raw.items()}


def collect_panel_edits(row_idx: int) -> dict:
    """Read the current state of the edit panel widgets for a given row.

    Must be defined before first use (called early in the script to collect
    widget state at the start of each Streamlit rerun).
    """
    sens = {}
    for sens_type in SENSITIVITY_TYPES:
        if st.session_state.get(f"sens_cb_{row_idx}_{sens_type}", False):
            label = st.session_state.get(
                f"sens_qual_{row_idx}_{sens_type}", QUALIFICATION_OPTIONS[0]
            )
            sens[sens_type] = QUALIFICATION_TO_VALUE.get(label, 0.0)

    blocks = {}
    for block_type in BLOCKER_TYPES:
        if st.session_state.get(f"block_cb_{row_idx}_{block_type}", False):
            label = st.session_state.get(
                f"block_qual_{row_idx}_{block_type}", QUALIFICATION_OPTIONS[0]
            )
            blocks[block_type] = QUALIFICATION_TO_VALUE.get(label, 0.0)

    return {"sensitivities": sens, "barriers": blocks}


# == Load data =================================================================

df = fetch_view_data()

if df.empty:
    st.warning("No data returned from STREAMLIT_APP_VIEW2.")
    st.stop()

# Initialise session state on first load or after explicit refresh.
if "original_df" not in st.session_state or st.session_state.get("force_reload"):
    st.session_state.original_df = df.copy(deep=True)
    st.session_state.working_df = df.copy(deep=True)
    st.session_state.active_edit_row = None
    st.session_state.active_info_row = None
    st.session_state.panel_edits = {}  # {row_idx: {"sensitivities": {...}, "barriers": {...}}}
    st.session_state.pending_changes = None
    st.session_state.submit_result = None
    st.session_state.force_reload = False


# == Collect panel edits FIRST (before building display) =======================
# Streamlit updates st.session_state widget values at the START of each rerun,
# so we can read the current panel state here, before the data_editor renders.
# This fixes the "one step behind" display lag.

if st.session_state.active_edit_row is not None:
    active = st.session_state.active_edit_row
    # Only collect widget state if the panel was already rendered on a previous
    # rerun (i.e. the widgets exist).  On the first open, the flag won't be set
    # yet so we skip — this prevents overwriting the original raw data with
    # empty values from non-existent widgets.
    if st.session_state.get(f"_panel_rendered_{active}", False):
        st.session_state.panel_edits[active] = collect_panel_edits(active)


# == Apply live preview: update display strings from accumulated panel edits ===

working = st.session_state.working_df

for idx, pe in st.session_state.panel_edits.items():
    if idx < len(working):
        sens_labels = _panel_edit_to_labels(pe, "sensitivities")
        block_labels = _panel_edit_to_labels(pe, "barriers")
        working.at[idx, "SENSITIVITIES"] = _labels_to_display(sens_labels)
        working.at[idx, "BARRIERS"] = _labels_to_display(block_labels)

# Add EDIT and INFO checkbox columns -- tick the active rows
working_with_edit = working.copy()
working_with_edit.insert(0, "EDIT", False)
working_with_edit.insert(1, "INFO", False)
active_row = st.session_state.active_edit_row
if active_row is not None and active_row < len(working_with_edit):
    working_with_edit.at[active_row, "EDIT"] = True
active_info = st.session_state.active_info_row
if active_info is not None and active_info < len(working_with_edit):
    working_with_edit.at[active_info, "INFO"] = True


# == Search ====================================================================

search_col1, search_col2 = st.columns([1, 1])

with search_col1:
    company_options = ["All"] + sorted(working_with_edit["COMPANY"].dropna().unique().tolist())
    selected_company = st.selectbox(
        "Search company",
        options=company_options,
        index=0,
        key="company_search",
    )

with search_col2:
    # Combine unique KAM values from both columns (defensive if columns missing from stale cache)
    has_kam_cols = "EEX_KAM" in working_with_edit.columns and "INCUBEX_KAM" in working_with_edit.columns
    if has_kam_cols:
        eex_kams = working_with_edit["EEX_KAM"].dropna().unique().tolist()
        incubex_kams = working_with_edit["INCUBEX_KAM"].dropna().unique().tolist()
        all_kams = sorted(set(eex_kams + incubex_kams))
    else:
        all_kams = []
    kam_options = ["All"] + all_kams
    selected_kam = st.selectbox(
        "Search KAM",
        options=kam_options,
        index=0,
        key="kam_search",
    )

# Apply filters (AND logic when both are set)
display_df = working_with_edit

if selected_company != "All":
    display_df = display_df[display_df["COMPANY"] == selected_company]

if selected_kam != "All" and has_kam_cols:
    kam_mask = (display_df["EEX_KAM"] == selected_kam) | (display_df["INCUBEX_KAM"] == selected_kam)
    display_df = display_df[kam_mask]


# == Column preset filter =====================================================

selected_preset = st.segmented_control(
    "Column Filters:",
    options=list(COLUMN_PRESETS.keys()),
    default="All",
    key="column_preset",
)

# Clear edit panel if the selected preset hides the EDIT column
preset_columns = COLUMN_PRESETS.get(selected_preset or "All")
if preset_columns is not None and "EDIT" not in preset_columns:
    if st.session_state.get("active_edit_row") is not None:
        st.session_state.active_edit_row = None


# == Date-based colour coding ==================================================

def highlight_entry_date(val):
    """Colour ENTRY_DATE cells by age: green <=14d, yellow 15-60d, red 60+d."""
    if pd.isna(val):
        return ""
    try:
        entry = pd.Timestamp(val)
        days_old = (pd.Timestamp(datetime.now()) - entry).days
    except Exception:
        return ""
    if days_old <= 14:
        return "background-color: #90EE90"
    elif days_old <= 60:
        return "background-color: #FFFFE0"
    else:
        return "background-color: #FFB6C6"


styled_df = display_df.style.applymap(highlight_entry_date, subset=["ENTRY_DATE"])


# == Data editor ===============================================================

# Build full column order, including KAM columns only when present in the data
_col_order_prefix = ["COMPANY", "CLIENT_TYPE", "CLIENT_STATUS"]
if has_kam_cols:
    _col_order_prefix += ["EEX_KAM", "INCUBEX_KAM"]

_full_col_order = _col_order_prefix + [
    "SENSITIVITIES", "BARRIERS", "EDIT",
    "DECISION_MAKERS", "EUA_VOLUME", "GO_VOLUME",
    "OTHER_PRODUCT_NOTES", "ACCESS_TYPE", "FRONT_END", "FRONT_END_DETAILS",
    "CLEARERS", "BROKERS", "ETRM", "SOURCE", "NOTES", "ENTRY_DATE",
    "INFO",  # Always far right
]

# Apply column preset filter
if preset_columns is None:
    # "All" preset — show everything
    _col_order = _full_col_order
else:
    # Filter to only the columns in the preset, preserving preset order
    _col_order = [c for c in preset_columns if c in _full_col_order]
    # Always append INFO (Last Update Dates) at the end
    if "INFO" not in _col_order:
        _col_order.append("INFO")

edited_df = st.data_editor(
    styled_df,
    column_config=get_column_config(),
    column_order=_col_order,
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    disabled=READ_ONLY_COLUMNS,
    key="client_data_editor",
)


# == Panel enforcement (one panel at a time: Edit OR Info) =====================

def _resolve_checkbox(col_name, prev_active_key, other_active_key):
    """Resolve a checkbox column, ensuring only one row is active and
    closing the other panel type if this one is newly activated.

    Returns the new active row index (or None).
    """
    ticked = edited_df.index[edited_df[col_name] == True].tolist()
    prev = st.session_state[prev_active_key]

    if ticked:
        if prev is not None and prev in ticked:
            others = [i for i in ticked if i != prev]
            new_active = others[0] if others else prev
        else:
            new_active = ticked[0]
        return new_active
    else:
        return None


new_edit = _resolve_checkbox("EDIT", "active_edit_row", "active_info_row")
new_info = _resolve_checkbox("INFO", "active_info_row", "active_edit_row")

needs_rerun = False

# -- Edit column changed
if new_edit != st.session_state.active_edit_row:
    # Clear rendered flag for the old panel row (if any)
    old_edit = st.session_state.active_edit_row
    if old_edit is not None:
        st.session_state.pop(f"_panel_rendered_{old_edit}", None)
    st.session_state.active_edit_row = new_edit
    if new_edit is not None:
        # Close info panel when edit panel opens
        st.session_state.active_info_row = None
    needs_rerun = True

# -- Info column changed
if new_info != st.session_state.active_info_row:
    st.session_state.active_info_row = new_info
    if new_info is not None:
        # Close edit panel when info panel opens
        st.session_state.active_edit_row = None
    needs_rerun = True

if needs_rerun:
    st.rerun()


# == Edit panel for Sensitivities & Blockers ===================================

def render_edit_panel(row_idx: int):
    """Render the Sensitivities + Blockers edit panel for one row."""
    orig_row = st.session_state.original_df.iloc[row_idx]
    company = orig_row["COMPANY"]
    client_type = orig_row["CLIENT_TYPE"]

    # Pre-populate from accumulated panel_edits if the user has actually interacted
    # with the panel (i.e. the edit contains data).  On the very first open,
    # collect_panel_edits stores {"sensitivities": {}, "barriers": {}} because the
    # widgets don't exist yet — we must fall through to the original raw data in
    # that case so existing values are shown.
    existing_pe = st.session_state.panel_edits.get(row_idx)
    pe_has_data = (
        existing_pe
        and (existing_pe.get("sensitivities") or existing_pe.get("barriers"))
    )
    if pe_has_data:
        sens_labels = _panel_edit_to_labels(existing_pe, "sensitivities")
        block_labels = _panel_edit_to_labels(existing_pe, "barriers")
    else:
        sens_labels = _raw_dict_to_labels(orig_row.get("_SENSITIVITIES_RAW", {}))
        block_labels = _raw_dict_to_labels(orig_row.get("_BARRIERS_RAW", {}))

    with st.container(border=True):
        st.markdown(
            f'<p class="sub-header">Editing: {company} ({client_type})</p>',
            unsafe_allow_html=True,
        )

        col_sens, col_block = st.columns(2)

        # -- Sensitivities --------------------------------------------------
        with col_sens:
            st.markdown("**Sensitivities**")
            for sens_type in SENSITIVITY_TYPES:
                c1, c2 = st.columns([1, 2])
                default_checked = sens_type in sens_labels
                with c1:
                    checked = st.checkbox(
                        sens_type,
                        value=default_checked,
                        key=f"sens_cb_{row_idx}_{sens_type}",
                    )
                with c2:
                    if checked:
                        default_qual = sens_labels.get(sens_type, QUALIFICATION_OPTIONS[0])
                        default_idx = (
                            QUALIFICATION_OPTIONS.index(default_qual)
                            if default_qual in QUALIFICATION_OPTIONS
                            else 0
                        )
                        st.selectbox(
                            f"{sens_type} level",
                            options=QUALIFICATION_OPTIONS,
                            index=default_idx,
                            key=f"sens_qual_{row_idx}_{sens_type}",
                            label_visibility="collapsed",
                        )

        # -- Blockers -------------------------------------------------------
        with col_block:
            st.markdown("**Blockers**")
            for block_type in BLOCKER_TYPES:
                c1, c2 = st.columns([1, 2])
                default_checked = block_type in block_labels
                with c1:
                    checked = st.checkbox(
                        block_type,
                        value=default_checked,
                        key=f"block_cb_{row_idx}_{block_type}",
                    )
                with c2:
                    if checked:
                        default_qual = block_labels.get(block_type, QUALIFICATION_OPTIONS[0])
                        default_idx = (
                            QUALIFICATION_OPTIONS.index(default_qual)
                            if default_qual in QUALIFICATION_OPTIONS
                            else 0
                        )
                        st.selectbox(
                            f"{block_type} level",
                            options=QUALIFICATION_OPTIONS,
                            index=default_idx,
                            key=f"block_qual_{row_idx}_{block_type}",
                            label_visibility="collapsed",
                        )


# == Info panel — per-field last-updated dates =================================

def render_info_panel(row_idx: int):
    """Show when each field was last updated for a given row.

    Reads per-field dates from the _FIELD_UPDATE_DATES_RAW column
    (a dict parsed from the FIELD_UPDATE_DATES JSON column on the view).
    """
    orig_row = st.session_state.original_df.iloc[row_idx]
    company = orig_row["COMPANY"]
    client_type = orig_row["CLIENT_TYPE"]
    update_dates = orig_row.get("_FIELD_UPDATE_DATES_RAW", {}) or {}

    with st.container(border=True):
        st.markdown(
            f'<p class="sub-header">Field update dates: {company} ({client_type})</p>',
            unsafe_allow_html=True,
        )

        if not update_dates:
            st.info("No update history found for this record.")
            return

        # Build a two-column layout of field -> date
        col1, col2 = st.columns(2)
        items = list(ALL_FIELDS_LABELS.items())
        mid = (len(items) + 1) // 2

        for i, (label, db_col) in enumerate(items):
            raw_date = update_dates.get(db_col)

            if raw_date is None or (isinstance(raw_date, float) and pd.isna(raw_date)):
                date_str = "-"
            else:
                try:
                    date_str = pd.Timestamp(raw_date).strftime("%d %b %Y")
                except Exception:
                    date_str = str(raw_date)

            target = col1 if i < mid else col2
            with target:
                st.markdown(f"**{label}:** {date_str}")


# == Render the active panel (only one at a time) =============================

if st.session_state.active_edit_row is not None:
    render_edit_panel(st.session_state.active_edit_row)
    # Mark that widgets now exist so collect_panel_edits runs on the next rerun
    st.session_state[f"_panel_rendered_{st.session_state.active_edit_row}"] = True
elif st.session_state.active_info_row is not None:
    render_info_panel(st.session_state.active_info_row)


# == Change detection ==========================================================

def _normalise(value):
    """Collapse NaN / None / empty-string into None for comparison."""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _dicts_equal(a, b) -> bool:
    """Compare two dicts treating None / empty dict as equal."""
    a = a if isinstance(a, dict) else {}
    b = b if isinstance(b, dict) else {}
    return a == b


def detect_changes(
    original: pd.DataFrame, edited: pd.DataFrame, panel_edits: dict
) -> list[dict]:
    """
    Compare every editable cell and panel field.
    Returns a list of INSERT-ready dicts (one per changed row).
    Unchanged fields are set to None (-> SQL NULL).

    Uses the DataFrame index (not positional iloc) so that filtering
    via the search box doesn't break row identity.
    """
    changed_rows: list[dict] = []

    # Iterate over the indices present in the edited DataFrame.
    # When a search filter is active, only the visible rows are compared.
    # Panel edits for non-visible rows are also checked separately below.
    checked_indices = set()

    for idx in edited.index:
        checked_indices.add(idx)
        orig_row = original.loc[idx]
        edit_row = edited.loc[idx]
        row_changed = False

        row_data: dict = {
            "company": edit_row["COMPANY"],
            "client_type": edit_row["CLIENT_TYPE"],
            "overall_volume": None,
            "power_volume": None,
            "gas_volume": None,
        }

        # -- Inline columns -------------------------------------------------
        for col in EDITABLE_COLUMNS:
            orig_val = _normalise(orig_row[col])
            edit_val = _normalise(edit_row[col])

            if col in ("EUA_VOLUME", "GO_VOLUME"):
                orig_num = None if orig_val is None else float(orig_val)
                edit_num = None if edit_val is None else float(edit_val)
                if orig_num != edit_num:
                    row_changed = True
                    row_data[col.lower()] = int(edit_num) if edit_num is not None else None
                else:
                    row_data[col.lower()] = None
            else:
                if orig_val != edit_val:
                    row_changed = True
                    row_data[col.lower()] = edit_val
                else:
                    row_data[col.lower()] = None

        # -- Panel-edited columns (Sensitivities & Blockers) ----------------
        if idx in panel_edits:
            pe = panel_edits[idx]

            orig_sens = orig_row.get("_SENSITIVITIES_RAW", {}) or {}
            new_sens = pe.get("sensitivities", {})
            if not _dicts_equal(orig_sens, new_sens):
                row_changed = True
                row_data["sensitivities"] = json.dumps(new_sens) if new_sens else None
            else:
                row_data["sensitivities"] = None

            orig_barriers = orig_row.get("_BARRIERS_RAW", {}) or {}
            new_barriers = pe.get("barriers", {})
            if not _dicts_equal(orig_barriers, new_barriers):
                row_changed = True
                row_data["barriers"] = json.dumps(new_barriers) if new_barriers else None
            else:
                row_data["barriers"] = None
        else:
            row_data["sensitivities"] = None
            row_data["barriers"] = None

        if row_changed:
            changed_rows.append(row_data)

    # Also check panel edits for rows NOT currently visible (filtered out).
    for idx, pe in panel_edits.items():
        if idx in checked_indices:
            continue
        orig_row = original.loc[idx]
        row_data = {
            "company": orig_row["COMPANY"],
            "client_type": orig_row["CLIENT_TYPE"],
            "overall_volume": None,
            "power_volume": None,
            "gas_volume": None,
        }
        # No inline edits for filtered-out rows; set all to None
        for col in EDITABLE_COLUMNS:
            row_data[col.lower()] = None

        row_changed = False
        orig_sens = orig_row.get("_SENSITIVITIES_RAW", {}) or {}
        new_sens = pe.get("sensitivities", {})
        if not _dicts_equal(orig_sens, new_sens):
            row_changed = True
            row_data["sensitivities"] = json.dumps(new_sens) if new_sens else None
        else:
            row_data["sensitivities"] = None

        orig_barriers = orig_row.get("_BARRIERS_RAW", {}) or {}
        new_barriers = pe.get("barriers", {})
        if not _dicts_equal(orig_barriers, new_barriers):
            row_changed = True
            row_data["barriers"] = json.dumps(new_barriers) if new_barriers else None
        else:
            row_data["barriers"] = None

        if row_changed:
            changed_rows.append(row_data)

    return changed_rows


# == Buttons ===================================================================

col_left, col_mid, col_right = st.columns([1, 2, 1])

with col_mid:
    submit_clicked = st.button("Submit Changes", type="primary", use_container_width=True)

with col_right:
    refresh_clicked = st.button("Refresh Data", use_container_width=True)

if refresh_clicked:
    fetch_view_data.clear()
    st.session_state.force_reload = True
    st.rerun()

# == Confirmation dialog =======================================================

@st.dialog("Confirm Submission", width="large")
def confirm_submit_dialog():
    """Modal popup showing a preview of changes before writing to Snowflake."""
    changed = st.session_state.pending_changes
    st.markdown(f"**{len(changed)} row(s) changed** — review before submitting:")

    skip_keys = {"company", "client_type", "overall_volume", "power_volume", "gas_volume"}
    preview_rows = []
    for r in changed:
        for k, v in r.items():
            if k in skip_keys or v is None:
                continue
            display_val = f"{v:,}" if isinstance(v, (int, float)) else str(v)
            preview_rows.append({
                "Company": r["company"],
                "Client Type": r["client_type"],
                "Field": k.replace("_", " ").title(),
                "New Value": display_val,
            })
    preview = pd.DataFrame(preview_rows)
    st.dataframe(preview, use_container_width=True, hide_index=True)

    btn_accept, btn_cancel = st.columns(2)
    with btn_accept:
        if st.button("Accept", type="primary", use_container_width=True):
            with st.spinner("Submitting changes..."):
                success_count, errors = insert_changed_rows(changed)
            st.session_state.submit_result = {
                "success": success_count, "errors": errors,
            }
            st.session_state.pending_changes = None
            if success_count > 0:
                fetch_view_data.clear()
                st.session_state.force_reload = True
            st.rerun()
    with btn_cancel:
        if st.button("Continue Editing", use_container_width=True):
            st.session_state.pending_changes = None
            st.rerun()


if submit_clicked:
    changed = detect_changes(
        st.session_state.original_df, edited_df, st.session_state.panel_edits
    )
    if not changed:
        st.info("No changes detected.")
    else:
        st.session_state.pending_changes = changed
        confirm_submit_dialog()

# Show result messages after a successful dialog submission
if st.session_state.get("submit_result"):
    result = st.session_state.pop("submit_result")
    if result["errors"]:
        for err in result["errors"]:
            st.error(err)
    if result["success"] > 0:
        st.success(f"Successfully submitted {result['success']} row(s).")
        st.balloons()
