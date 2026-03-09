from __future__ import annotations

import logging

from typing import TYPE_CHECKING, Any, Dict, List

import pandas as pd
import requests

if TYPE_CHECKING:
    import datetime as dt

logger = logging.getLogger(__name__)


DSHIELD_PORTHISTORY = 'https://isc.sans.edu/api/porthistory/{port}/{start}/{end}?json'


class DshieldClient:
    """
    Client for retrieving port history time series from SANS ISC / DShield.

    This is used as a botnet activity proxy (e.g., sources/day scanning telnet ports).
    """

    def __init__(self, user_agent: str, timeout_s: int = 45) -> None:
        """
        Args:
            user_agent: Custom User-Agent string; ISC prefers non-default UAs.
            timeout_s: HTTP request timeout in seconds.
        """
        self._headers = {
            'User-Agent': user_agent,
            'Accept': 'application/json',
        }
        self._timeout_s = timeout_s

    def fetch_port_history(
        self,
        port: int,
        start: dt.date,
        end: dt.date,
        metric: str,
    ) -> pd.DataFrame:
        """
        Fetch a daily time series for a given port over a date range.

        Args:
            port: TCP/UDP port of interest (e.g., 23, 2323, 7547).
            start: Start date (YYYY-MM-DD).
            end: End date (YYYY-MM-DD).
            metric: One of 'records', 'sources', 'targets', 'tcp', 'udp' depending on response.

        Returns:
            DataFrame with columns: date (datetime), value (int), port (int).
        """
        allowed = {'records', 'sources', 'targets', 'tcp', 'udp'}
        if metric not in allowed:
            raise ValueError(f'Unsupported DShield metric: {metric}. Allowed: {sorted(allowed)}')

        url = DSHIELD_PORTHISTORY.format(port=port, start=start.isoformat(), end=end.isoformat())

        logger.debug('DShield request: port=%d metric=%s url=%s', port, metric, url)

        r = requests.get(url, headers=self._headers, timeout=self._timeout_s)
        logger.debug('DShield response: port=%d status=%d', port, r.status_code)
        r.raise_for_status()

        payload = r.json()

        portinfo = self._extract_portinfo_list(payload)
        if not portinfo:
            if isinstance(payload, dict):
                keys = list(payload.keys())
                logger.debug(
                    'DShield payload type=dict keys_count=%d sample_keys=%s',
                    len(keys),
                    keys[:10],
                )
            else:
                logger.debug('DShield payload type=%s', type(payload).__name__)
            logger.debug('DShield raw response (first 800 chars): %s', r.text[:800])
        logger.debug('DShield parsed portinfo entries: port=%d entries=%d', port, len(portinfo))

        rows: List[Dict[str, Any]] = []
        for item in portinfo:
            d = item.get('date')
            if not d:
                continue
            raw = item.get(metric, 0)
            try:
                value = int(str(raw).replace(',', '').strip())
            except Exception:
                value = 0
            rows.append({'date': pd.to_datetime(d), 'value': value, 'port': port})

        df = pd.DataFrame(rows)
        logger.debug('DShield dataframe built: port=%d rows=%d', port, len(df))
        if not df.empty:
            logger.debug('DShield df sample (port=%d):\n%s', port, df.head(5).to_string(index=False))
        else:
            logger.warning(
                'DShield returned no rows for port=%d metric=%s range=%s..%s',
                port,
                metric,
                start.isoformat(),
                end.isoformat(),
            )
        if df.empty:
            return df

        df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    @staticmethod
    def _extract_portinfo_list(payload: Any) -> List[Dict[str, Any]]:
        """
        Extract the daily-entry list from the DShield / ISC response.

        Supports multiple observed shapes:
        1) payload is a list[dict] (already the timeseries)
        2) payload["portinfo"] is list/dict
        3) payload["porthistory"] is dict/list and contains "portinfo"
        4) payload is dict keyed by numeric strings ("0","1",...) whose values
            are daily dict entries (observed from /api/porthistory/*?json)
        """
        # Case 1: payload is already a list of entries
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]

        if not isinstance(payload, dict):
            return []

        def _as_list(x: Any) -> List[Dict[str, Any]]:
            if isinstance(x, list):
                return [y for y in x if isinstance(y, dict)]
            if isinstance(x, dict):
                return [x]
            return []

        # Case 2: top-level portinfo
        if 'portinfo' in payload:
            out = _as_list(payload.get('portinfo'))
            if out:
                return out

        # Case 3: nested under porthistory
        ph = payload.get('porthistory')
        if isinstance(ph, dict):
            out = _as_list(ph.get('portinfo'))
            if out:
                return out
            for k in ('data', 'history', 'timeseries', 'series'):
                out = _as_list(ph.get(k))
                if out:
                    return out

        # Case 4: list of dicts
        if isinstance(ph, list):
            for item in ph:
                if isinstance(item, dict):
                    out = _as_list(item.get('portinfo'))
                    if out:
                        return out

        # Case 5: numeric-key dict
        numeric_entries: List[Dict[str, Any]] = []
        for k, v in payload.items():
            if isinstance(k, str) and k.isdigit() and isinstance(v, dict):
                numeric_entries.append(v)

        if numeric_entries:

            def _sort_key(d: Dict[str, Any]) -> str:
                return str(d.get('date', ''))

            return sorted(numeric_entries, key=_sort_key)

        return []
