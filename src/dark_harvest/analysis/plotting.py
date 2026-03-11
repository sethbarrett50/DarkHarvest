from __future__ import annotations

from typing import TYPE_CHECKING

import matplotlib.pyplot as plt

if TYPE_CHECKING:
    from pathlib import Path

    import pandas as pd


def plot_event_study(summary_df: pd.DataFrame, out_path: Path) -> None:
    """
    Plot event-study mean trajectory with a 95% confidence interval.

    Args:
        summary_df: DataFrame containing relative_day, mean_new_devices,
            ci_low, and ci_high.
        out_path: Output PNG path.
    """
    fig = plt.figure()
    ax = plt.gca()

    ax.plot(summary_df['relative_day'], summary_df['mean_new_devices'])
    ax.fill_between(
        summary_df['relative_day'],
        summary_df['ci_low'],
        summary_df['ci_high'],
        alpha=0.2,
    )
    ax.axvline(0, linestyle='--', alpha=0.6)
    ax.set_xlabel('Relative day')
    ax.set_ylabel('Mean botnet proxy')
    ax.set_title('Event study around outage starts')

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


def plot_cross_correlation(cc_df: pd.DataFrame, out_path: Path) -> None:
    """
    Plot lagged cross-correlation.

    Args:
        cc_df: DataFrame containing lag_days and correlation.
        out_path: Output PNG path.
    """
    fig = plt.figure()
    ax = plt.gca()

    ax.plot(cc_df['lag_days'], cc_df['correlation'])
    ax.axvline(0, linestyle='--', alpha=0.6)
    ax.set_xlabel('Lag (days)')
    ax.set_ylabel('Pearson correlation')
    ax.set_title('Lagged cross-correlation')

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


def plot_permutation_histogram(
    null_df: pd.DataFrame,
    observed_effect: float,
    out_path: Path,
) -> None:
    """
    Plot permutation null distribution and observed statistic.

    Args:
        null_df: DataFrame with a null_effect column.
        observed_effect: Observed post-minus-pre effect.
        out_path: Output PNG path.
    """
    fig = plt.figure()
    ax = plt.gca()

    ax.hist(null_df['null_effect'], bins=40)
    ax.axvline(observed_effect, linestyle='--', alpha=0.8)
    ax.set_xlabel('Effect size')
    ax.set_ylabel('Frequency')
    ax.set_title('Permutation null distribution')

    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close(fig)
