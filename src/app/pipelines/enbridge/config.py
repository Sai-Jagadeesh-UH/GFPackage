"""Enbridge-specific constants and configuration.

URLs, CSS selectors, column mappings, flow indicator maps,
date formats, and blob path templates used across all Enbridge
scraping, munging, and pushing logic.
"""

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

INFOPOST_URL = "https://infopost.enbridge.com/infopost/"

PARENT_PIPE = "Enbridge"


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
    "B": "N",
    "Delivery": "D",
    "Receipt": "R",
    "Storage Injection": "D",
    "Storage Withdrawal": "R",
}
# N - Neutral flow (bidirectional or unknown)
# Z - Need SME review
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
