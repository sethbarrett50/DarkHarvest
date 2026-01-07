from __future__ import annotations

import datetime as dt

from typing import Any, List, Optional, Tuple

import requests

from dateutil import parser as dateparser

from src.models import Incident

GCP_INCIDENTS_JSON = 'https://status.cloud.google.com/incidents.json'


def _to_dt(x: Any) -> Optional[dt.datetime]:
    """
    Convert a timestamp-like value to a UTC-naive datetime.

    - If parsed datetime is timezone-aware, convert to UTC and drop tzinfo.
    - If parsed datetime is naive, keep as-is.
    """
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        parsed = x
    else:
        s = str(x).strip()
        if not s:
            return None
        parsed = dateparser.parse(s)

    if parsed is None:
        return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)

    return parsed


def _clamp_range(
    inc_start: dt.datetime,
    inc_end: dt.datetime,
    start: dt.datetime,
    end: dt.datetime,
) -> Optional[Tuple[dt.datetime, dt.datetime]]:
    """Clamp an incident window to [start, end] if overlapping; otherwise return None."""
    if inc_end < start or inc_start > end:
        return None
    return max(inc_start, start), min(inc_end, end)


def fetch_gcp_incidents(start: dt.datetime, end: dt.datetime) -> List[Incident]:
    """
    Fetch Google Cloud incidents from the public incidents JSON.

    Args:
        start: Inclusive start datetime bound.
        end: Inclusive end datetime bound.

    Returns:
        List of Incident objects clamped to [start, end].
    """
    r = requests.get(GCP_INCIDENTS_JSON, timeout=45)
    r.raise_for_status()
    items = r.json()

    incidents: List[Incident] = []
    for item in items:
        inc_id = str(item.get('number', '')) or str(item.get('id', '')) or str(item.get('external_desc', ''))
        title = str(item.get('title', '')) or str(item.get('service_name', 'GCP incident'))
        url = str(item.get('uri', ''))

        begin = _to_dt(item.get('begin'))
        finish = _to_dt(item.get('end')) or _to_dt(item.get('most_recent_update'))
        if begin is None:
            continue

        inc_start = begin
        inc_end = finish or begin
        sev = str(item.get('severity', '')) or str(item.get('impact', ''))

        clamped = _clamp_range(inc_start, inc_end, start, end)
        if clamped is None:
            continue

        incidents.append(
            Incident(
                provider='GCP',
                incident_id=str(inc_id) if inc_id else f'gcp-{begin.isoformat()}',
                title=title,
                start=clamped[0],
                end=clamped[1],
                severity=sev,
                url=url,
            )
        )

    return incidents
