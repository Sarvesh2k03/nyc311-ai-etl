"""Upstream "source system" header dialects.

The row values are real NYC 311 Open Data. The *column names* are re-cased and
re-abbreviated here to simulate the same payload arriving from four different
upstream systems - which is the situation the AI schema mapper exists to solve.
Keeping the simulation in one file makes it obvious what is real (the data) and
what is synthesized (the headers), and lets you add a fifth dialect in one edit
to test the mapper against genuinely unseen naming.

`extra_columns` are deliberate distractors: operational/audit columns that map
to NOTHING in the canonical schema. A mapper that maps them is over-mapping,
which is worse than leaving them out, so they are the negative test case.
"""
from __future__ import annotations

from dataclasses import dataclass

# Socrata API field -> canonical field. The single source of truth for what a
# correct mapping is, used only by extract (to build files) and by the accuracy
# check in tests - never by the mapper itself.
SOCRATA_TO_CANONICAL: dict[str, str] = {
    "unique_key": "request_id",
    "created_date": "created_at",
    "closed_date": "closed_at",
    "agency": "agency_code",
    "agency_name": "agency_name",
    "complaint_type": "complaint_type",
    "descriptor": "descriptor",
    "location_type": "location_type",
    "incident_zip": "postal_code",
    "incident_address": "street_address",
    "city": "city",
    "borough": "borough",
    "status": "status",
    "resolution_description": "resolution_description",
    "resolution_action_updated_date": "resolution_updated_at",
    "community_board": "community_board",
    "open_data_channel_type": "intake_channel",
    "latitude": "latitude",
    "longitude": "longitude",
}


@dataclass(frozen=True)
class SourceDialect:
    name: str
    description: str
    headers: dict[str, str]            # canonical field -> this system's header
    extra_columns: tuple[str, ...] = ()  # unmappable audit columns


LEGACY_CRM = SourceDialect(
    name="legacy_crm",
    description="1990s mainframe CRM export: uppercase, vowel-dropped abbreviations.",
    headers={
        "request_id": "SR_NUMBER",
        "created_at": "DT_OPENED",
        "closed_at": "DT_CLOSED",
        "agency_code": "AGCY",
        "agency_name": "AGCY_DESC",
        "complaint_type": "CMPLNT_TYP",
        "descriptor": "DESCR",
        "location_type": "LOC_TYP",
        "postal_code": "ZIP_CD",
        "street_address": "ADDR_LINE_1",
        "city": "CITY_NM",
        "borough": "BORO",
        "status": "SR_STATUS",
        "resolution_description": "RES_TXT",
        "resolution_updated_at": "DT_RES_UPD",
        "community_board": "CB_CODE",
        "intake_channel": "INTK_SRC",
        "latitude": "GEO_LAT",
        "longitude": "GEO_LON",
    },
    extra_columns=("ROW_CHECKSUM", "MAINFRAME_LOAD_SEQ"),
)

VENDOR_CSV = SourceDialect(
    name="vendor_csv",
    description="Third-party vendor CSV: Title Case headers with spaces.",
    headers={
        "request_id": "Service Request ID",
        "created_at": "Date Received",
        "closed_at": "Date Resolved",
        "agency_code": "Agency Acronym",
        "agency_name": "Responsible Department",
        "complaint_type": "Issue Category",
        "descriptor": "Issue Detail",
        "location_type": "Premises Type",
        "postal_code": "Postal Code",
        "street_address": "Street Address",
        "city": "Town",
        "borough": "District",
        "status": "Case State",
        "resolution_description": "Closing Remarks",
        "resolution_updated_at": "Last Update Of Outcome",
        "community_board": "Community District",
        "intake_channel": "Submission Method",
        "latitude": "Latitude",
        "longitude": "Longitude",
    },
    extra_columns=("Vendor Batch Ref", "Export Timestamp"),
)

MOBILE_APP = SourceDialect(
    name="mobile_app_v2",
    description="Mobile app v2 JSON-to-CSV export: camelCase headers.",
    headers={
        "request_id": "srId",
        "created_at": "openedTimestamp",
        "closed_at": "closedTimestamp",
        "agency_code": "assignedAgencyCode",
        "agency_name": "assignedAgencyLabel",
        "complaint_type": "reportCategory",
        "descriptor": "reportSubCategory",
        "location_type": "siteKind",
        "postal_code": "zip",
        "street_address": "addressLine",
        "city": "cityName",
        "borough": "boroughName",
        "status": "ticketState",
        "resolution_description": "agencyOutcomeNotes",
        "resolution_updated_at": "outcomeUpdatedAt",
        "community_board": "cbIdentifier",
        "intake_channel": "originChannel",
        "latitude": "geoLat",
        "longitude": "geoLng",
    },
    extra_columns=("appVersion", "deviceOs"),
)

PARTNER_API = SourceDialect(
    name="partner_api",
    description="Modern partner API already close to the canonical contract.",
    headers={
        "request_id": "request_id",
        "created_at": "created_at",
        "closed_at": "closed_at",
        "agency_code": "agency_code",
        "agency_name": "agency_name",
        "complaint_type": "complaint_type",
        "descriptor": "descriptor",
        "location_type": "location_type",
        "postal_code": "postal_code",
        "street_address": "street_address",
        "city": "city",
        "borough": "borough",
        "status": "status",
        "resolution_description": "resolution_description",
        "resolution_updated_at": "resolution_updated_at",
        "community_board": "community_board",
        "intake_channel": "intake_channel",
        "latitude": "latitude",
        "longitude": "longitude",
    },
    extra_columns=("etl_partition_key",),
)

DIALECTS: tuple[SourceDialect, ...] = (LEGACY_CRM, VENDOR_CSV, MOBILE_APP, PARTNER_API)
DIALECTS_BY_NAME: dict[str, SourceDialect] = {d.name: d for d in DIALECTS}


def expected_mapping(dialect: SourceDialect) -> dict[str, str | None]:
    """Ground truth: source header -> canonical field (None for distractors)."""
    truth: dict[str, str | None] = {h: c for c, h in dialect.headers.items()}
    truth.update({c: None for c in dialect.extra_columns})
    return truth
