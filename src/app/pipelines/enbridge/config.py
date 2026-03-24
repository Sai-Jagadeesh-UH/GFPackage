"""Enbridge-specific constants and configuration.

URLs, CSS selectors, column mappings, flow indicator maps,
date formats, and blob path templates used across all Enbridge
scraping, munging, and pushing logic.
"""

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

RTBA_BASE = "https://rtba.enbridge.com/InformationalPosting/Default.aspx"
INFOPOST_BASE = "https://infopost.enbridge.com/InfoPost"
METADATA_BASE = "https://linkwc.enbridge.com/Pointdata"

PARENT_PIPE = "Enbridge"


def rtba_url(pipe_code: str, data_type: str = "OA") -> str:
    """Short-way URL: rtba.enbridge.com."""
    return f"{RTBA_BASE}?bu={pipe_code}&Type={data_type}"


def infopost_home_url(pipe_code: str) -> str:
    """Long-way entry URL: infopost.enbridge.com."""
    return f"{INFOPOST_BASE}/{pipe_code}Home.asp?Pipe={pipe_code}"


def metadata_url(meta_code: str) -> str:
    """Direct CSV download URL for metadata point lists."""
    return f"{METADATA_BASE}/{meta_code}AllPoints.csv"


# ---------------------------------------------------------------------------
# CSS / Playwright selectors
# ---------------------------------------------------------------------------

DATE_BOX_LABEL = "Gas Date: "
IFRAME_SELECTOR = "#ContentPaneIframe"
CAPACITY_MENU_SELECTOR = "li#Capacity.dropdown.sidebar-menu-item"
OA_LINK_NAME = "Operationally Available"
DOWNLOAD_LINK_NAME = "Downloadable Format"
DOWNLOAD_CSV_TEXT = "Download Csv"
SG_MAPS_TEXT = "Operational Capacity Maps"

# ---------------------------------------------------------------------------
# Pipe codes with special handling
# ---------------------------------------------------------------------------

# MNUS always uses the long-way scrape path
LONG_WAY_ONLY_CODES: set[str] = {"MNUS"}

# WE not available before this date
WE_AVAILABILITY_DATE = "2025-10-01"

# TETLP Lease NJ/NY is a known SG segment to skip
SG_SKIP_SEGMENTS: set[str] = {"TETLP Lease NJ/NY"}


# ---------------------------------------------------------------------------
# OA column mapping (raw CSV column → gold schema column)
# ---------------------------------------------------------------------------
"""
Cycle_Desc,Post_Date,Eff_Gas_Day,Cap_Type_Desc,Post_Time,Eff_Time,Loc,Loc_Name,Loc_Zn,Flow_Ind_Desc,Loc_Purp_Desc,Loc_QTI_Desc,Meas_Basis_Desc,IT,All_Qty_Avail,Total_Design_Capacity,Operating_Capacity,Total_Scheduled_Quantity,Operationally_Available_Capacity,TSP_Name,TSP

"""
OA_RAW_COLUMNS = [
    "Cycle_Desc", "Eff_Gas_Day", "Loc", "Loc_Name", "Loc_Zn",
    "Flow_Ind_Desc", "Loc_Purp_Desc", "IT", "All_Qty_Avail",
    "Total_Design_Capacity", "Operating_Capacity",
    "Total_Scheduled_Quantity", "Operationally_Available_Capacity",
]

OA_RENAME_MAP = {
    "Cycle_Desc": "CycleDesc",
    "Loc_Name": "LocName",
    "Loc_Zn": "LocZn",
    "Loc_Purp_Desc": "LocPurpDesc",
    "All_Qty_Avail": "AllQtyAvail",
    "IT": "IT",
}

OA_DATE_FORMAT = "%m-%d-%Y"

OA_FLOW_MAP: dict[str, str] = {
    "Delivery": "D",
    "Receipt": "R",
    "Storage Injection": "D",
    "Storage Withdrawal": "R",
}

# ---------------------------------------------------------------------------
# SG (Segment Capacity) column mapping
# ---------------------------------------------------------------------------

SG_RAW_COLUMNS = ["Station Name", "Cap", "Nom", "Cap2"]

SG_DATE_FORMAT = "%Y%m%d"

SG_FLOW_MAP: dict[str, str] = {
    "TD1": "F",
    "TD2": "B",
}

# ---------------------------------------------------------------------------
# ST (Storage Capacity) column mapping — same format as OA
# ---------------------------------------------------------------------------

ST_DATE_FORMAT = OA_DATE_FORMAT
ST_FLOW_MAP: dict[str, str] = OA_FLOW_MAP

# ---------------------------------------------------------------------------
# NN column mapping
# ---------------------------------------------------------------------------

NN_RAW_COLUMNS = [
    "Effective_From_DateTime", "Loc_Prop", "Loc_Name",
    "Allocated_Qty", "Direction_Of_Flow", "Accounting_Physincal_Indicator",
]

NN_DATE_FORMAT = "%m/%d/%Y %H:%M"

NN_FLOW_MAP: dict[str, str] = {
    "D": "D",
    "R": "R",
    "B": "B",
    "Delivery": "D",
    "Receipt": "R",
    "Storage Injection": "D",
    "Storage Withdrawal": "R",
}

# ---------------------------------------------------------------------------
# NN scrape date offset (NN data lags by 4 days)
# ---------------------------------------------------------------------------

NN_DATE_LAG_DAYS = 4

# ---------------------------------------------------------------------------
# Blob path templates
# ---------------------------------------------------------------------------
# File naming patterns:
#   OA: {PipeCode}_OA_{YYYYMMDD}_INTRDY_{YYYY-MM-DD}_{HHMM}.csv
#   SG: {PipeCode}_SG{n}_{YYYYMMDD}_INTRDY_{YYYY-MM-DD}_{HHMM}.csv
#   ST: {PipeCode}_ST_{YYYYMMDD}_INTRDY_{YYYY-MM-DD}_{HHMM}.csv
#   NN: {PipeCode}_NN_{YYYYMMDD}.csv
#   META: {PipeCode}_AllPoints.csv


def oa_bronze_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/PointCapacity/{eff_date[:-2]}/{filename}"


def sg_bronze_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/SegmentCapacity/{eff_date[:-2]}/{filename}"


def st_bronze_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/StorageCapacity/{eff_date[:-2]}/{filename}"


def nn_bronze_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/NoNotice/{eff_date[:-4]}/{filename}"


def meta_bronze_blob_path(filename: str) -> str:
    return f"Enbridge/Metadata/{filename}"


def oa_silver_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/PointCapacity/{eff_date[:-2]}/{filename}"


def sg_silver_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/SegmentCapacity/{eff_date[:-2]}/{filename}"


def st_silver_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/StorageCapacity/{eff_date[:-2]}/{filename}"


def nn_silver_blob_path(eff_date: str, filename: str) -> str:
    return f"Enbridge/NoNotice/{eff_date[:-10]}/{filename}"
