from __future__ import annotations

import datetime as dt

from typing import Any, List, Optional, Tuple

import requests

from dateutil import parser as dateparser

from src.models import Incident

CLOUDFLARE_INCIDENTS_JSON = 'https://www.cloudflarestatus.com/api/v2/incidents.json'


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


def fetch_cloudflare_incidents(start: dt.datetime, end: dt.datetime) -> List[Incident]:
    """
    Fetch Cloudflare incidents from their Statuspage API.

    Args:
        start: Inclusive start datetime bound.
        end: Inclusive end datetime bound.

    Returns:
        List of Incident objects clamped to [start, end].
    """
    r = requests.get(CLOUDFLARE_INCIDENTS_JSON, timeout=45)
    r.raise_for_status()
    data = r.json()

    incidents: List[Incident] = []
    for item in data.get('incidents', []):
        inc_id = str(item.get('id', ''))
        title = str(item.get('name', ''))
        url = str(item.get('shortlink', '')) or str(item.get('url', ''))

        created = _to_dt(item.get('created_at'))
        resolved = _to_dt(item.get('resolved_at'))
        updated = _to_dt(item.get('updated_at'))

        if created is None:
            continue

        inc_start = created
        inc_end = resolved or updated or created
        sev = str(item.get('impact', ''))

        clamped = _clamp_range(inc_start, inc_end, start, end)
        if clamped is None:
            continue

        incidents.append(
            Incident(
                provider='Cloudflare',
                incident_id=inc_id,
                title=title,
                start=clamped[0],
                end=clamped[1],
                severity=sev,
                url=url,
            )
        )

    return incidents
