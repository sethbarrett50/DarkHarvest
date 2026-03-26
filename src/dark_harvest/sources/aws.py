from __future__ import annotations

import calendar
import datetime as dt
import logging
import re

from typing import TYPE_CHECKING, Dict, List, Tuple

import feedparser
import requests

from bs4 import BeautifulSoup
from dateutil import parser as dateparser

from dark_harvest.models import Incident

if TYPE_CHECKING:
    import time

logger = logging.getLogger(__name__)

AWS_RSS_ALL = 'https://status.aws.amazon.com/rss/all.rss'
AWS_STATUSGATOR_HISTORY_URL = 'https://statusgator.com/services/amazon-web-services/outage-history'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': ('text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'),
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
}


def _ensure_utc_naive(value: dt.datetime) -> dt.datetime:
    """Normalize a datetime to UTC-naive."""
    if value.tzinfo is None:
        return value
    return value.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _to_dt(value: str | dt.datetime | None) -> dt.datetime | None:
    """Parse a datetime-like value into UTC-naive datetime."""
    if value is None:
        return None

    if isinstance(value, dt.datetime):
        return _ensure_utc_naive(value)

    raw = value.strip()
    if not raw:
        return None

    parsed = dateparser.parse(raw)
    return _ensure_utc_naive(parsed)


def _struct_time_to_dt(value: time.struct_time | tuple | None) -> dt.datetime | None:
    """Convert feedparser parsed time tuple to UTC-naive datetime."""
    if value is None:
        return None

    try:
        timestamp = calendar.timegm(value)
    except (TypeError, OverflowError, ValueError) as exc:
        logger.debug('Failed converting struct_time %r: %s', value, exc)
        return None

    return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).replace(tzinfo=None)


def _clamp_range(
    inc_start: dt.datetime,
    inc_end: dt.datetime,
    start: dt.datetime,
    end: dt.datetime,
) -> Tuple[dt.datetime, dt.datetime] | None:
    """Clamp an incident window to [start, end] if overlapping."""
    if inc_end < start or inc_start > end:
        return None
    return max(inc_start, start), min(inc_end, end)


def _aws_incident_key_from_guid(guid: str) -> str:
    """Extract an AWS incident key from an RSS entry guid/id."""
    if '#' in guid:
        return guid.split('#', 1)[1].strip()
    return guid.strip()


def _parse_statusgator_severity(block_text: str) -> str:
    """Infer severity from StatusGator text block."""
    lowered = block_text.lower()
    if 'major icon major' in lowered or lowered.startswith('major'):
        return 'major'
    if 'minor icon minor' in lowered or lowered.startswith('minor'):
        return 'minor'
    if 'maintenance' in lowered:
        return 'maintenance'
    return ''


def _parse_statusgator_incident_dates(
    block_lines: List[str],
) -> tuple[dt.datetime | None, dt.datetime | None]:
    """
    Parse incident start/end timestamps from a StatusGator outage block.

    We prefer the explicit:
      - Detected by StatusGator: ...
      - Outage ended: ...
    lines when available.
    """
    inc_start: dt.datetime | None = None
    inc_end: dt.datetime | None = None

    for line in block_lines:
        clean = line.strip()

        if (
            clean.startswith('Detected by StatusGator:')
            or clean.startswith('Officially acknowledged:')
            and inc_start is None
        ):
            raw = clean.split(':', 1)[1].strip()
            inc_start = _to_dt(raw)

        elif clean.startswith('Outage ended:'):
            raw = clean.split(':', 1)[1].strip()
            raw = re.sub(r'\s*\([^)]*\)\s*$', '', raw).strip()
            inc_end = _to_dt(raw)

    return inc_start, inc_end


def _extract_statusgator_incident_blocks(text: str) -> List[List[str]]:
    """
    Break a StatusGator outage history page into incident-sized blocks.

    Each incident on the page begins with a severity marker such as:
      - 'minor icon Minor'
      - 'major icon Major'
    """
    lines = [line.strip() for line in text.splitlines()]
    blocks: List[List[str]] = []
    current: List[str] = []

    severity_re = re.compile(r'^(minor|major|maintenance)\s+icon\s+', re.IGNORECASE)

    for line in lines:
        if not line:
            continue

        if severity_re.match(line):
            if current:
                blocks.append(current)
            current = [line]
            continue

        if current:
            current.append(line)

    if current:
        blocks.append(current)

    return blocks


def _parse_statusgator_incident_block(
    block_lines: List[str],
    page_number: int,
    item_number: int,
    start: dt.datetime,
    end: dt.datetime,
) -> Incident | None:
    """Parse one incident block from a StatusGator outage-history page."""
    block_text = '\n'.join(block_lines)

    severity = _parse_statusgator_severity(block_text)

    date_line: str | None = None
    title_line: str | None = None
    summary_lines: List[str] = []

    for line in block_lines:
        if date_line is None and re.match(r'^[A-Za-z]+\s+\d{1,2},\s+\d{4}$', line):
            date_line = line
            continue

        if line.startswith('##'):
            title_line = line.removeprefix('##').strip()
            continue

        if (
            not line.startswith('Detected by StatusGator:')
            and not line.startswith('Officially acknowledged:')
            and not line.startswith('Outage ended:')
            and not re.match(r'^(minor|major|maintenance)\s+icon\s+', line, re.IGNORECASE)
            and line != date_line
            and not line.startswith('Report an outage')
        ):
            summary_lines.append(line)

    inc_start, inc_end = _parse_statusgator_incident_dates(block_lines)

    if inc_start is None and date_line is not None:
        inc_start = _to_dt(date_line)

    if inc_start is None:
        logger.debug(
            'Skipping StatusGator block without parseable start: page=%d item=%d title=%r',
            page_number,
            item_number,
            title_line,
        )
        return None

    if inc_end is None:
        inc_end = inc_start

    clamped = _clamp_range(inc_start, inc_end, start, end)
    if clamped is None:
        return None

    title = title_line or 'AWS outage'
    incident_id = f'statusgator-p{page_number}-{item_number}-{int(inc_start.timestamp())}'

    return Incident(
        provider='AWS',
        incident_id=incident_id,
        title=title,
        start=clamped[0],
        end=clamped[1],
        severity=severity,
        url=f'{AWS_STATUSGATOR_HISTORY_URL}?page={page_number}',
    )


def _fetch_aws_incidents_statusgator(
    start: dt.datetime,
    end: dt.datetime,
    max_pages: int = 36,
) -> List[Incident]:
    """
    Fetch AWS outage history from StatusGator.

    StatusGator's outage-history pages are paginated, so we iterate page=1..N
    until we stop finding incidents or we move past the requested date window.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    incidents: List[Incident] = []
    pages_fetched = 0

    logger.debug(
        'Fetching AWS incidents from StatusGator history: start=%s end=%s',
        start.isoformat(),
        end.isoformat(),
    )

    for page in range(1, max_pages + 1):
        url = AWS_STATUSGATOR_HISTORY_URL
        params = {'page': page} if page > 1 else None

        response = session.get(url, params=params, timeout=45)
        response.raise_for_status()

        pages_fetched += 1
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text('\n', strip=True)

        blocks = _extract_statusgator_incident_blocks(page_text)
        logger.debug(
            'StatusGator page=%d fetched: html_len=%d incident_blocks=%d',
            page,
            len(html),
            len(blocks),
        )

        if not blocks:
            break

        page_incidents: List[Incident] = []
        page_oldest_start: dt.datetime | None = None

        for idx, block in enumerate(blocks, start=1):
            incident = _parse_statusgator_incident_block(
                block_lines=block,
                page_number=page,
                item_number=idx,
                start=start,
                end=end,
            )
            if incident is None:
                continue

            page_incidents.append(incident)

            if page_oldest_start is None or incident.start < page_oldest_start:
                page_oldest_start = incident.start

        logger.debug(
            'StatusGator page=%d parsed incidents in range=%d',
            page,
            len(page_incidents),
        )

        incidents.extend(page_incidents)

        if page_oldest_start is not None and page_oldest_start < start:
            logger.debug(
                'Stopping StatusGator pagination at page=%d because oldest_start=%s is older than requested start=%s',
                page,
                page_oldest_start.isoformat(),
                start.isoformat(),
            )
            break

    logger.info(
        'AWS incidents from StatusGator: %d across %d page(s)',
        len(incidents),
        pages_fetched,
    )
    return incidents


def _fetch_aws_incidents_rss(start: dt.datetime, end: dt.datetime) -> List[Incident]:
    """Fetch recent AWS incidents from the public AWS RSS feed."""
    logger.debug('Fetching AWS RSS feed: %s', AWS_RSS_ALL)
    feed = feedparser.parse(AWS_RSS_ALL)

    entries = list(getattr(feed, 'entries', []))
    logger.debug('AWS RSS raw entries: %d', len(entries))

    grouped: Dict[str, List[Tuple[dt.datetime, str, str]]] = {}

    for entry in entries:
        guid = str(getattr(entry, 'guid', '') or getattr(entry, 'id', '') or '').strip()
        title = str(getattr(entry, 'title', '') or '').strip()
        published = _struct_time_to_dt(
            getattr(entry, 'published_parsed', None) or getattr(entry, 'updated_parsed', None)
        )

        if not guid or published is None:
            logger.debug(
                'Skipping AWS RSS entry guid=%r title=%r published=%r updated=%r',
                guid,
                title,
                getattr(entry, 'published', None),
                getattr(entry, 'updated', None),
            )
            continue

        key = _aws_incident_key_from_guid(guid)
        grouped.setdefault(key, []).append((published, title, guid))

    incidents: List[Incident] = []

    for key, updates in grouped.items():
        updates_sorted = sorted(updates, key=lambda item: item[0])
        inc_start = updates_sorted[0][0]
        inc_end = updates_sorted[-1][0]
        title = updates_sorted[-1][1]
        url = f'https://status.aws.amazon.com/#{key}'

        severity = ''
        if re.search(r'\bresolved\b', title, flags=re.IGNORECASE):
            severity = 'resolved'
        elif re.search(
            r'\b(service disruption|service degradation|increased errors)\b',
            title,
            flags=re.IGNORECASE,
        ):
            severity = 'impact'

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
                severity=severity,
                url=url,
            )
        )

    logger.debug('AWS RSS incidents returned: %d', len(incidents))
    return incidents


def _dedupe_incidents(incidents: List[Incident]) -> List[Incident]:
    """Deduplicate incidents by provider/title/start/end."""
    seen: set[tuple[str, str, dt.datetime, dt.datetime]] = set()
    deduped: List[Incident] = []

    for incident in sorted(incidents, key=lambda x: (x.start, x.end, x.title)):
        key = (
            incident.provider,
            incident.title,
            incident.start,
            incident.end,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(incident)

    return deduped


def fetch_aws_incidents(start: dt.datetime, end: dt.datetime) -> List[Incident]:
    """
    Fetch AWS incidents.

    Primary source:
        - StatusGator outage history pages (multi-year external history)

    Fallback / supplement:
        - AWS RSS feed (recent official updates)
    """
    start = _ensure_utc_naive(start)
    end = _ensure_utc_naive(end)

    incidents: List[Incident] = []

    try:
        sg_incidents = _fetch_aws_incidents_statusgator(start, end)
        incidents.extend(sg_incidents)
        logger.debug('AWS incidents from StatusGator: %d', len(sg_incidents))
    except Exception as exc:
        logger.warning('StatusGator AWS fetch failed: %s', exc)

    try:
        rss_incidents = _fetch_aws_incidents_rss(start, end)
        incidents.extend(rss_incidents)
        logger.debug('AWS incidents from RSS fallback: %d', len(rss_incidents))
    except Exception as exc:
        logger.warning('AWS RSS fetch failed: %s', exc)

    deduped = _dedupe_incidents(incidents)
    logger.info('AWS incidents returned total: %d', len(deduped))
    return deduped
