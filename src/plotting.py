from __future__ import annotations

from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import pandas as pd

if TYPE_CHECKING:
    import datetime as dt


def plot_overlay(
    outages: pd.DataFrame,
    botnet_daily: pd.DataFrame,
    start: dt.datetime,
    end: dt.datetime,
    out_path: str,
) -> None:
    """
    Plot botnet proxy time series and overlay outage windows as shaded spans.

    Args:
        outages: DataFrame of incidents with columns start/end/provider.
        botnet_daily: DataFrame with columns date/new_devices.
        start: Plot start datetime.
        end: Plot end datetime.
        out_path: Output image path.
    """
    fig = plt.figure()
    ax = plt.gca()

    ax.plot(botnet_daily['date'], botnet_daily['new_devices'])
    ax.set_xlabel('Date')
    ax.set_ylabel('Botnet proxy metric (per day)')

    if not outages.empty:
        providers = list(outages['provider'].unique())
        for p in providers:
            sub = outages[outages['provider'] == p]
            for _, row in sub.iterrows():
                ax.axvspan(row['start'], row['end'], alpha=0.15, label=p)

        handles, labels = ax.get_legend_handles_labels()
        seen = set()
        uniq_handles = []
        uniq_labels = []
        for handle, label in zip(handles, labels):
            if label in seen:
                continue
            seen.add(label)
            uniq_handles.append(handle)
            uniq_labels.append(label)

        if uniq_labels:
            ax.legend(uniq_handles, uniq_labels, loc='upper right')

    ax.set_xlim(pd.to_datetime(start), pd.to_datetime(end))  # type: ignore
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
