from __future__ import annotations

import datetime as dt
import re

from typing import Any, Dict, List, Optional, Tuple

import feedparser

from dateutil import parser as dateparser

from src.models import Incident

AWS_RSS_ALL = 'https://status.aws.amazon.com/rss/all.rss'


def _to_dt(x: Any) -> Optional[dt.datetime]:
    """Convert an arbitrary timestamp-like value to a datetime (UTC-naive)."""
    if x is None:
        return None
    if isinstance(x, dt.datetime):
        return x
    s = str(x).strip()
    if not s:
        return None
    return dateparser.parse(s)


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


def _aws_incident_key_from_guid(guid: str) -> str:
    """Extract an AWS incident key from an RSS entry guid/id."""
    if '#' in guid:
        return guid.split('#', 1)[1].strip()
    return guid.strip()


def fetch_aws_incidents(start: dt.datetime, end: dt.datetime) -> List[Incident]:
    """
    Fetch AWS Health Dashboard incidents from the AWS Status RSS feed.

    This groups RSS updates by incident key and uses the earliest/last timestamp
    as the incident window. This is an approximation but works well for overlaying.

    Args:
        start: Inclusive start datetime bound.
        end: Inclusive end datetime bound.

    Returns:
        List of Incident objects clamped to [start, end].
    """
    feed = feedparser.parse(AWS_RSS_ALL)

    grouped: Dict[str, List[Tuple[dt.datetime, str, str]]] = {}

    for entry in getattr(feed, 'entries', []):
        guid = str(getattr(entry, 'guid', '') or getattr(entry, 'id', '') or '')
        title = str(getattr(entry, 'title', '') or '')
        published = _to_dt(getattr(entry, 'published', None) or getattr(entry, 'updated', None))
        if not guid or published is None:
            continue

        key = _aws_incident_key_from_guid(guid)
        grouped.setdefault(key, []).append((published, title, guid))

    incidents: List[Incident] = []
    for key, updates in grouped.items():
        updates_sorted = sorted(updates, key=lambda t: t[0])
        inc_start = updates_sorted[0][0]
        inc_end = updates_sorted[-1][0]
        title = updates_sorted[-1][1]
        url = 'http://status.aws.amazon.com/#' + key

        sev = ''
        if re.search(r'\b(resolved)\b', title, flags=re.IGNORECASE):
            sev = 'resolved'
        elif re.search(
            r'\b(service disruption|service degradation|increased errors)\b',
            title,
            flags=re.IGNORECASE,
        ):
            sev = 'impact'

        clamped = _clamp_range(inc_start, inc_end, start, end)
        if clamped is None:
            continue

        incidents.append(
            Incident(
                provider='AWS',
                incident_id=key,
                title=title,
                start=clamped[0],
                end=clamped[1],
                severity=sev,
                url=url,
            )
        )

    return incidents
