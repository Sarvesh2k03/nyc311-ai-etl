"""The canonical target schema.

Every upstream source system, whatever it calls its columns, is mapped onto
these fields before anything is loaded. The descriptions and synonyms here are
the only "knowledge" the mapper has about the target - they are fed verbatim
into the LLM prompt and used as the fuzzy-matching corpus, so improving a
description improves both paths at once.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CanonicalField:
    name: str
    dtype: str          # logical type: string | integer | float | timestamp
    description: str
    synonyms: tuple[str, ...] = field(default_factory=tuple)
    required: bool = False


CANONICAL_FIELDS: tuple[CanonicalField, ...] = (
    CanonicalField("request_id", "string",
                   "Unique identifier of the service request, one per complaint.",
                   ("unique key", "service request number", "sr id", "case id"), required=True),
    CanonicalField("created_at", "timestamp",
                   "Timestamp the service request was opened/received by the city.",
                   ("created date", "date opened", "opened timestamp", "date received"), required=True),
    CanonicalField("closed_at", "timestamp",
                   "Timestamp the service request was closed. Null while still open.",
                   ("closed date", "date closed", "date resolved", "completion date")),
    CanonicalField("agency_code", "string",
                   "Short acronym of the responding city agency, e.g. NYPD, DSNY, HPD.",
                   ("agency", "agency acronym", "agcy", "dept code"), required=True),
    CanonicalField("agency_name", "string",
                   "Full human-readable name of the responding agency.",
                   ("agency description", "department name", "agcy desc")),
    CanonicalField("complaint_type", "string",
                   "Primary category of the complaint, e.g. 'Noise - Residential'.",
                   ("complaint category", "issue type", "request type", "problem type"), required=True),
    CanonicalField("descriptor", "string",
                   "Secondary detail refining the complaint type, e.g. 'Loud Music/Party'.",
                   ("sub type", "complaint detail", "description of issue", "subcategory")),
    CanonicalField("location_type", "string",
                   "Kind of place the incident occurred, e.g. 'Residential Building', 'Street'.",
                   ("place type", "site type", "premises type")),
    CanonicalField("postal_code", "string",
                   "5-digit ZIP code of the incident location.",
                   ("incident zip", "zip", "zip code", "postcode")),
    CanonicalField("street_address", "string",
                   "Street address line of the incident location.",
                   ("incident address", "address line 1", "street addr")),
    CanonicalField("city", "string",
                   "City or town name of the incident location.",
                   ("city name", "town", "municipality")),
    CanonicalField("borough", "string",
                   "One of the five NYC boroughs: BROOKLYN, QUEENS, MANHATTAN, BRONX, STATEN ISLAND.",
                   ("boro", "district", "nyc borough")),
    CanonicalField("status", "string",
                   "Lifecycle state of the request: Open, Closed, In Progress, Assigned, Pending.",
                   ("sr status", "case status", "state", "request state")),
    CanonicalField("resolution_description", "string",
                   "Free-text narrative of how the agency resolved the request.",
                   ("resolution", "res desc", "outcome notes", "closing remarks")),
    CanonicalField("resolution_updated_at", "timestamp",
                   "Timestamp the resolution text was last updated by the agency.",
                   ("resolution action updated date", "date resolution updated", "res upd dt")),
    CanonicalField("community_board", "string",
                   "NYC community board that covers the incident, e.g. '01 BROOKLYN'.",
                   ("cb", "cb code", "community district", "community board code")),
    CanonicalField("intake_channel", "string",
                   "How the request reached the city: PHONE, ONLINE, MOBILE, UNKNOWN.",
                   ("open data channel type", "channel", "source channel", "submission method")),
    CanonicalField("latitude", "float",
                   "WGS84 latitude of the incident, roughly 40.4 to 41.0 for NYC.",
                   ("lat", "y coordinate", "geo lat")),
    CanonicalField("longitude", "float",
                   "WGS84 longitude of the incident, roughly -74.3 to -73.6 for NYC.",
                   ("lon", "lng", "x coordinate", "geo long")),
)

CANONICAL_NAMES: tuple[str, ...] = tuple(f.name for f in CANONICAL_FIELDS)
CANONICAL_BY_NAME: dict[str, CanonicalField] = {f.name: f for f in CANONICAL_FIELDS}
REQUIRED_FIELDS: tuple[str, ...] = tuple(f.name for f in CANONICAL_FIELDS if f.required)

# Columns added by the pipeline itself, not mapped from any source.
LINEAGE_COLUMNS: tuple[str, ...] = (
    "_source_system",   # which upstream dialect the row came from
    "_batch_date",      # logical partition date of the batch
    "_batch_id",        # unique id of the pipeline run that landed the row
    "_ingested_at",     # wall-clock load time
)


def schema_for_prompt() -> str:
    """Render the canonical schema as compact text for the LLM prompt."""
    lines = []
    for f in CANONICAL_FIELDS:
        syn = f"; also called: {', '.join(f.synonyms)}" if f.synonyms else ""
        lines.append(f"- {f.name} ({f.dtype}): {f.description}{syn}")
    return "\n".join(lines)
