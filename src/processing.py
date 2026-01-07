from __future__ import annotations

import logging

from typing import TYPE_CHECKING, Iterable, List

import pandas as pd

if TYPE_CHECKING:
    import datetime as dt

    from src.models import Incident
    from src.sources.dshield import DshieldClient

logger = logging.getLogger(__name__)


def incidents_to_df(incs: Iterable[Incident]) -> pd.DataFrame:
    """
    Convert incidents to a normalized DataFrame suitable for CSV export/plotting.
    """
    rows = []
    for i in incs:
        rows.append({
            'provider': i.provider,
            'incident_id': i.incident_id,
            'title': i.title,
            'start': i.start,
            'end': i.end,
            'duration_minutes': (i.end - i.start).total_seconds() / 60.0,
            'severity': i.severity,
            'url': i.url,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(['start', 'provider']).reset_index(drop=True)
    return df


def build_botnet_proxy_series(
    client: DshieldClient,
    ports: List[int],
    start: dt.datetime,
    end: dt.datetime,
    metric: str,
) -> pd.DataFrame:
    """
    Build a daily botnet activity proxy by summing a DShield metric across ports.

    Args:
        client: DshieldClient instance.
        ports: Ports to include.
        start: Start datetime bound (date portion used).
        end: End datetime bound (date portion used).
        metric: DShield metric ('records', 'sources', 'targets', 'tcp', 'udp').

    Returns:
        DataFrame with columns: date, new_devices (proxy).
    """
    start_date = start.date()
    end_date = end.date()

    logger.info('Building botnet proxy series across %d ports', len(ports))

    frames: List[pd.DataFrame] = []
    for p in ports:
        dfp = client.fetch_port_history(p, start_date, end_date, metric)
        if not dfp.empty:
            frames.append(dfp)

    if not frames:
        logger.warning('No botnet proxy data collected from DShield (all ports empty).')
        return pd.DataFrame(columns=['date', 'new_devices'])

    all_df = pd.concat(frames, ignore_index=True)
    daily = all_df.groupby('date', as_index=False)['value'].sum().rename(columns={'value': 'new_devices'})  # type: ignore

    full_days = pd.date_range(start=start_date, end=end_date, freq='D')
    daily = daily.set_index('date').reindex(full_days, fill_value=0).rename_axis('date').reset_index()

    daily['date'] = pd.to_datetime(daily['date']).dt.tz_localize(None)
    daily['new_devices'] = pd.to_numeric(daily['new_devices'], errors='coerce').fillna(0).astype(int)
    return daily
