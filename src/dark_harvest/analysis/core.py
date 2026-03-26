from __future__ import annotations

import json
import logging

from typing import TYPE_CHECKING, Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm

from .models import (
    AnalysisArtifacts,
    AnalysisConfig,
    CrossCorrelationResult,
    EventStudyResult,
    PermutationTestResult,
    RegressionResult,
)
from .plotting import (
    plot_cross_correlation,
    plot_event_study,
    plot_permutation_histogram,
)

if TYPE_CHECKING:
    import datetime as dt

logger = logging.getLogger(__name__)


def build_analysis_frame(
    outages: pd.DataFrame,
    botnet_daily: pd.DataFrame,
    start: dt.datetime,
    end: dt.datetime,
) -> pd.DataFrame:
    """
    Build a daily analysis frame combining outage-derived features and the
    botnet activity proxy.

    Args:
        outages: Outage dataframe with start/end/provider columns.
        botnet_daily: Daily botnet series with date/new_devices columns.
        start: Inclusive analysis start date.
        end: Inclusive analysis end date.

    Returns:
        A daily dataframe containing botnet counts plus outage exposure
        features.
    """
    base_dates = pd.date_range(
        start=pd.Timestamp(start).normalize(),
        end=pd.Timestamp(end).normalize(),
        freq='D',
    )
    analysis_df = pd.DataFrame({'date': base_dates})

    botnet = botnet_daily.copy()
    botnet['date'] = pd.to_datetime(botnet['date']).dt.normalize()
    botnet['new_devices'] = pd.to_numeric(
        botnet['new_devices'],
        errors='coerce',
    ).fillna(0)

    daily_outage = _expand_incidents_to_daily(outages, start, end)

    analysis_df = analysis_df.merge(
        botnet[['date', 'new_devices']],
        on='date',
        how='left',
    )
    analysis_df = analysis_df.merge(
        daily_outage,
        on='date',
        how='left',
    )

    analysis_df['new_devices'] = analysis_df['new_devices'].fillna(0).astype(int)
    analysis_df['outage_count'] = analysis_df['outage_count'].fillna(0).astype(int)
    analysis_df['outage_minutes'] = pd.to_numeric(
        analysis_df['outage_minutes'],
        errors='coerce',
    ).fillna(0.0)
    analysis_df['provider_count'] = analysis_df['provider_count'].fillna(0).astype(int)
    analysis_df['outage_any'] = (analysis_df['outage_count'] > 0).astype(int)
    analysis_df['day_index'] = np.arange(len(analysis_df), dtype=int)
    analysis_df['day_of_week'] = analysis_df['date'].dt.dayofweek.astype(int)

    return analysis_df


def _expand_incidents_to_daily(
    outages: pd.DataFrame,
    start: dt.datetime,
    end: dt.datetime,
) -> pd.DataFrame:
    """
    Expand outage windows into daily overlap features.

    Args:
        outages: Outage dataframe.
        start: Inclusive start.
        end: Inclusive end.

    Returns:
        Daily outage-feature dataframe.
    """
    normalized_start = pd.Timestamp(start).normalize()
    normalized_end = pd.Timestamp(end).normalize()

    if outages.empty:
        return pd.DataFrame({
            'date': pd.date_range(
                start=normalized_start,
                end=normalized_end,
                freq='D',
            ),
            'outage_count': 0,
            'outage_minutes': 0.0,
            'provider_count': 0,
        })

    working = outages.copy()
    working['start'] = pd.to_datetime(working['start'], errors='coerce')
    working['end'] = pd.to_datetime(working['end'], errors='coerce')
    working = working.dropna(subset=['start', 'end'])

    rows: list[dict[str, object]] = []

    for row in working.to_dict(orient='records'):
        incident_start = pd.Timestamp(row['start'])
        incident_end = pd.Timestamp(row['end'])

        if incident_end < normalized_start or incident_start > normalized_end:
            continue

        span_start = max(incident_start, normalized_start)
        span_end = min(
            incident_end,
            normalized_end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        )

        for day in pd.date_range(
            start=span_start.normalize(),
            end=span_end.normalize(),
            freq='D',
        ):
            day_start = day
            day_end = day + pd.Timedelta(days=1)

            overlap_start = max(span_start, day_start)
            overlap_end = min(span_end, day_end)

            overlap_minutes = max(
                0.0,
                (overlap_end - overlap_start).total_seconds() / 60.0,
            )
            if overlap_minutes <= 0:
                continue

            rows.append({
                'date': day.normalize(),
                'provider': str(row['provider']),
                'incident_id': str(row['incident_id']),
                'overlap_minutes': overlap_minutes,
            })

    if not rows:
        return pd.DataFrame({
            'date': pd.date_range(
                start=normalized_start,
                end=normalized_end,
                freq='D',
            ),
            'outage_count': 0,
            'outage_minutes': 0.0,
            'provider_count': 0,
        })

    expanded = pd.DataFrame(rows)
    grouped = expanded.groupby('date', as_index=False).agg(
        outage_count=('incident_id', 'nunique'),
        outage_minutes=('overlap_minutes', 'sum'),
        provider_count=('provider', 'nunique'),
    )

    return grouped


def _event_start_dates(
    analysis_df: pd.DataFrame,
    collapse_consecutive_outages: bool,
) -> list[pd.Timestamp]:
    """
    Derive outage event start dates from the daily analysis frame.

    Args:
        analysis_df: Daily analysis dataframe.
        collapse_consecutive_outages: Whether to collapse consecutive outage
            days into a single event start.

    Returns:
        List of event-start dates.
    """
    if analysis_df.empty:
        return []

    working = analysis_df.copy()
    working['date'] = pd.to_datetime(working['date'], errors='coerce')
    working = working.dropna(subset=['date']).reset_index(drop=True)

    dates = list(working['date'])
    outages = list(working['outage_any'].astype(bool))

    if not any(outages):
        return []

    event_dates: list[pd.Timestamp] = []
    previous_outage = False

    for date, is_outage in zip(dates, outages, strict=False):
        if not is_outage:
            previous_outage = False
            continue

        event_date = pd.Timestamp(date)

        if not collapse_consecutive_outages or not previous_outage:
            event_dates.append(event_date)

        previous_outage = True

    return event_dates


def _collect_event_windows(
    analysis_df: pd.DataFrame,
    event_dates: Iterable[pd.Timestamp],
    window_days: int,
) -> pd.DataFrame:
    """
    Collect event windows centered on outage starts.

    Args:
        analysis_df: Daily merged analysis dataframe.
        event_dates: Event-start dates.
        window_days: Symmetric window size.

    Returns:
        Long-form dataframe with one row per event-relative-day observation.
    """
    frame = analysis_df.copy()
    frame = frame.sort_values('date').reset_index(drop=True)
    date_to_value = dict(zip(frame['date'], frame['new_devices']))

    if frame.empty:
        return pd.DataFrame()

    min_date = pd.Timestamp(frame['date'].min())
    max_date = pd.Timestamp(frame['date'].max())

    rows: list[dict[str, object]] = []
    event_id = 0

    for event_date in event_dates:
        event_date = pd.Timestamp(event_date).normalize()
        window_start = event_date - pd.Timedelta(days=window_days)
        window_end = event_date + pd.Timedelta(days=window_days)

        if window_start < min_date or window_end > max_date:
            logger.debug(
                'Skipping event at %s because full window is unavailable.',
                event_date.date(),
            )
            continue

        for relative_day in range(-window_days, window_days + 1):
            current_date = event_date + pd.Timedelta(days=relative_day)
            rows.append({
                'event_id': event_id,
                'event_date': event_date,
                'relative_day': relative_day,
                'date': current_date,
                'new_devices': float(date_to_value[current_date]),
            })

        event_id += 1

    return pd.DataFrame(rows)


def _compute_observed_effect(per_event_df: pd.DataFrame) -> float:
    """
    Compute the mean post-minus-pre event effect.

    Args:
        per_event_df: Long-form event-study dataframe.

    Returns:
        Mean event effect.
    """
    if per_event_df.empty:
        return 0.0

    effect_values: list[float] = []

    for _, sub in per_event_df.groupby('event_id'):
        pre = sub[sub['relative_day'] < 0]['new_devices']
        post = sub[sub['relative_day'] > 0]['new_devices']

        if pre.empty or post.empty:
            continue

        effect_values.append(float(post.mean() - pre.mean()))

    if not effect_values:
        return 0.0

    return float(np.mean(effect_values))


def run_event_study(
    analysis_df: pd.DataFrame,
    config: AnalysisConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    Run the event study.

    Args:
        analysis_df: Daily merged analysis frame.
        config: Analysis configuration.

    Returns:
        Tuple of:
            - per-event long dataframe
            - summary dataframe by relative day
            - observed post-minus-pre effect
    """
    event_dates = _event_start_dates(
        analysis_df=analysis_df,
        collapse_consecutive_outages=config.collapse_consecutive_outages,
    )

    per_event_df = _collect_event_windows(
        analysis_df=analysis_df,
        event_dates=event_dates,
        window_days=config.event_window_days,
    )

    if per_event_df.empty:
        summary_df = pd.DataFrame(
            columns=[
                'relative_day',
                'mean_new_devices',
                'std_new_devices',
                'count',
                'sem',
                'ci_low',
                'ci_high',
            ]
        )
        return per_event_df, summary_df, 0.0

    summary_df = (
        per_event_df
        .groupby('relative_day', as_index=False)
        .agg(
            mean_new_devices=('new_devices', 'mean'),
            std_new_devices=('new_devices', 'std'),
            count=('new_devices', 'count'),
        )
        .sort_values('relative_day')
        .reset_index(drop=True)
    )

    summary_df['std_new_devices'] = summary_df['std_new_devices'].fillna(0.0)
    summary_df['sem'] = np.where(
        summary_df['count'] > 1,
        summary_df['std_new_devices'] / np.sqrt(summary_df['count']),
        0.0,
    )
    summary_df['ci_low'] = summary_df['mean_new_devices'] - 1.96 * summary_df['sem']
    summary_df['ci_high'] = summary_df['mean_new_devices'] + 1.96 * summary_df['sem']

    observed_effect = _compute_observed_effect(per_event_df)
    return per_event_df, summary_df, observed_effect


def run_cross_correlation(
    analysis_df: pd.DataFrame,
    max_lag_days: int,
) -> pd.DataFrame:
    """
    Compute lagged correlation corr(outage_t, botnet_{t+k}).

    Positive lag means botnet activity occurs after outage exposure.

    Args:
        analysis_df: Daily merged analysis frame.
        max_lag_days: Maximum lag in days.

    Returns:
        DataFrame containing lag_days and correlation.
    """
    x = pd.to_numeric(analysis_df['outage_any'], errors='coerce').astype(float)
    y = pd.to_numeric(analysis_df['new_devices'], errors='coerce').astype(float)

    rows: list[dict[str, float | int]] = []

    for lag in range(-max_lag_days, max_lag_days + 1):
        shifted = y.shift(-lag)
        valid = ~(x.isna() | shifted.isna())

        x_valid = x.loc[valid].to_numpy(dtype=float)
        y_valid = shifted.loc[valid].to_numpy(dtype=float)

        if len(x_valid) < 3 or np.all(x_valid == x_valid[0]) or np.all(y_valid == y_valid[0]):
            corr = np.nan
        else:
            corr = float(np.corrcoef(x_valid, y_valid)[0, 1])

        rows.append({
            'lag_days': lag,
            'correlation': corr,
        })

    return pd.DataFrame(rows)


def run_permutation_test(
    analysis_df: pd.DataFrame,
    config: AnalysisConfig,
    n_events: int,
) -> tuple[pd.DataFrame, float]:
    """
    Run a permutation test on the event-study effect size.

    Args:
        analysis_df: Daily merged analysis frame.
        config: Analysis configuration.
        n_events: Number of observed events used in the event study.

    Returns:
        Tuple of:
            - null distribution dataframe
            - empirical two-sided p-value
    """
    rng = np.random.default_rng(config.random_seed)
    frame = analysis_df.sort_values('date').reset_index(drop=True)

    if frame.empty or n_events <= 0:
        return pd.DataFrame({'null_effect': []}), 1.0

    min_index = config.event_window_days
    max_index = len(frame) - config.event_window_days - 1

    if max_index < min_index:
        return pd.DataFrame({'null_effect': []}), 1.0

    valid_indices = np.arange(min_index, max_index + 1, dtype=int)

    observed_per_event, _, observed_effect = run_event_study(analysis_df, config)
    if observed_per_event.empty:
        return pd.DataFrame({'null_effect': []}), 1.0

    null_effects: list[float] = []

    for _ in range(config.n_permutations):
        sampled_indices = rng.choice(valid_indices, size=n_events, replace=False)
        sampled_dates = [pd.Timestamp(frame.iloc[i]['date']) for i in sampled_indices]

        perm_df = _collect_event_windows(
            analysis_df=frame,
            event_dates=sampled_dates,
            window_days=config.event_window_days,
        )
        effect = _compute_observed_effect(perm_df)
        null_effects.append(effect)

    null_df = pd.DataFrame({'null_effect': null_effects})

    p_value = float((np.abs(null_df['null_effect'].to_numpy()) >= abs(observed_effect)).mean())

    return null_df, p_value


def fit_count_regression(
    analysis_df: pd.DataFrame,
    regression_model: str,
) -> tuple[pd.DataFrame, str, str]:
    """
    Fit a count regression model for daily botnet activity.

    Args:
        analysis_df: Daily merged analysis frame.
        regression_model: One of auto, poisson, or negbin.

    Returns:
        Tuple of:
            - coefficient dataframe
            - text summary
            - fitted model name
    """
    model_df = analysis_df.copy()

    y = model_df['new_devices'].astype(float)

    dow_dummies = pd.get_dummies(
        model_df['day_of_week'].astype(int),
        prefix='dow',
        drop_first=True,
        dtype=float,
    )

    X = pd.concat(
        [
            model_df[['outage_any', 'outage_minutes', 'provider_count', 'day_index']].astype(float),
            dow_dummies,
        ],
        axis=1,
    )
    X = sm.add_constant(X, has_constant='add')

    selected_model = regression_model
    if regression_model == 'auto':
        selected_model = 'negbin' if float(y.var()) > float(y.mean()) * 1.5 else 'poisson'

    if selected_model == 'poisson':
        result = sm.GLM(y, X, family=sm.families.Poisson()).fit()
        model_name = 'Poisson'
    elif selected_model == 'negbin':
        result = sm.GLM(
            y,
            X,
            family=sm.families.NegativeBinomial(),
        ).fit()
        model_name = 'NegativeBinomial'
    else:
        raise ValueError(f'Unsupported regression model: {regression_model}. Expected one of auto, poisson, negbin.')

    coef_df = pd.DataFrame({
        'term': result.params.index,
        'coefficient': result.params.values,
        'std_err': result.bse.values,
        'z_or_t': result.tvalues,
        'p_value': result.pvalues,
        'exp_coefficient': np.exp(result.params.values),
    })

    return coef_df, result.summary().as_text(), model_name


def run_analysis_pipeline(
    outages_df: pd.DataFrame,
    botnet_daily: pd.DataFrame,
    start: dt.datetime,
    end: dt.datetime,
    config: AnalysisConfig,
) -> AnalysisArtifacts:
    """
    Run the full statistical analysis pipeline and write outputs to disk.

    Args:
        outages_df: Normalized outage dataframe.
        botnet_daily: Daily botnet proxy dataframe.
        start: Inclusive analysis start.
        end: Inclusive analysis end.
        config: Analysis configuration.

    Returns:
        Bundle of analysis artifact paths.
    """
    analysis_dir = config.output_dir / 'analysis'
    analysis_dir.mkdir(parents=True, exist_ok=True)

    analysis_df = build_analysis_frame(
        outages=outages_df,
        botnet_daily=botnet_daily,
        start=pd.Timestamp(start),
        end=pd.Timestamp(end),
    )
    analysis_frame_csv = analysis_dir / 'analysis_frame.csv'
    analysis_df.to_csv(analysis_frame_csv, index=False)

    logger.info('Running event study...')
    per_event_df, event_summary_df, observed_effect = run_event_study(
        analysis_df=analysis_df,
        config=config,
    )

    per_event_csv = analysis_dir / 'event_study_per_event.csv'
    event_summary_csv = analysis_dir / 'event_study_summary.csv'
    per_event_df.to_csv(per_event_csv, index=False)
    event_summary_df.to_csv(event_summary_csv, index=False)

    event_plot_path = analysis_dir / 'event_study.png'
    if not event_summary_df.empty:
        plot_event_study(event_summary_df, event_plot_path)

    event_result = EventStudyResult(
        per_event=per_event_csv,
        summary=event_summary_csv,
        observed_effect=observed_effect,
        n_events_used=int(per_event_df['event_id'].nunique()) if not per_event_df.empty else 0,
    )

    logger.info('Running cross-correlation...')
    cc_df = run_cross_correlation(
        analysis_df=analysis_df,
        max_lag_days=config.max_lag_days,
    )
    cc_csv = analysis_dir / 'cross_correlation.csv'
    cc_df.to_csv(cc_csv, index=False)

    cc_plot_path = analysis_dir / 'cross_correlation.png'
    if not cc_df.empty:
        plot_cross_correlation(cc_df, cc_plot_path)

    cc_non_null = cc_df.dropna(subset=['correlation'])
    if cc_non_null.empty:
        peak_lag_days = 0
        peak_correlation = 0.0
    else:
        peak_idx = int(cc_non_null['correlation'].abs().idxmax())
        peak_row = cc_non_null.loc[peak_idx]

        if isinstance(peak_row, pd.Series):
            peak_lag_days = int(pd.to_numeric(peak_row['lag_days']))
            peak_correlation = float(pd.to_numeric(peak_row['correlation']))
        else:
            peak_lag_days = 0
            peak_correlation = 0.0

    cc_result = CrossCorrelationResult(
        csv_path=cc_csv,
        peak_lag_days=peak_lag_days,
        peak_correlation=peak_correlation,
    )

    logger.info('Running permutation test...')
    null_df, p_value = run_permutation_test(
        analysis_df=analysis_df,
        config=config,
        n_events=event_result.n_events_used,
    )
    null_csv = analysis_dir / 'permutation_null.csv'
    null_df.to_csv(null_csv, index=False)

    permutation_plot_path = analysis_dir / 'permutation_hist.png'
    if not null_df.empty:
        plot_permutation_histogram(
            null_df=null_df,
            observed_effect=observed_effect,
            out_path=permutation_plot_path,
        )

    permutation_result = PermutationTestResult(
        csv_path=null_csv,
        observed_effect=observed_effect,
        p_value=p_value,
        n_permutations=config.n_permutations,
    )

    logger.info('Fitting count regression...')
    coef_df, regression_summary_text, model_name = fit_count_regression(
        analysis_df=analysis_df,
        regression_model=config.regression_model,
    )

    coefficients_csv = analysis_dir / 'regression_coefficients.csv'
    summary_txt = analysis_dir / 'regression_summary.txt'
    coef_df.to_csv(coefficients_csv, index=False)
    summary_txt.write_text(regression_summary_text, encoding='utf-8')

    regression_result = RegressionResult(
        summary_path=summary_txt,
        coefficients_path=coefficients_csv,
        model_name=model_name,
    )

    metadata_path = analysis_dir / 'analysis_metadata.json'
    metadata_path.write_text(
        json.dumps(
            {
                'config': {
                    'output_dir': str(config.output_dir),
                    'event_window_days': config.event_window_days,
                    'max_lag_days': config.max_lag_days,
                    'n_permutations': config.n_permutations,
                    'random_seed': config.random_seed,
                    'regression_model': config.regression_model,
                    'collapse_consecutive_outages': config.collapse_consecutive_outages,
                },
                'artifacts': {
                    'analysis_frame_csv': str(analysis_frame_csv),
                    'event_study_per_event_csv': str(per_event_csv),
                    'event_study_summary_csv': str(event_summary_csv),
                    'cross_correlation_csv': str(cc_csv),
                    'permutation_null_csv': str(null_csv),
                    'regression_coefficients_csv': str(coefficients_csv),
                    'regression_summary_txt': str(summary_txt),
                },
                'results': {
                    'event_study_observed_effect': observed_effect,
                    'event_count_used': event_result.n_events_used,
                    'cross_correlation_peak_lag_days': peak_lag_days,
                    'cross_correlation_peak_correlation': peak_correlation,
                    'permutation_p_value': p_value,
                    'regression_model_name': model_name,
                },
            },
            indent=2,
        ),
        encoding='utf-8',
    )

    return AnalysisArtifacts(
        analysis_frame_csv=analysis_frame_csv,
        event_study=event_result,
        cross_correlation=cc_result,
        permutation_test=permutation_result,
        regression=regression_result,
        event_study_plot=event_plot_path,
        cross_correlation_plot=cc_plot_path,
        permutation_plot=permutation_plot_path,
    )
