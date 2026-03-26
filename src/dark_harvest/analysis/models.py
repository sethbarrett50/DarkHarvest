from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from pathlib import Path


RegressionModel = Literal['auto', 'poisson', 'negbin']


@dataclass(frozen=True)
class AnalysisConfig:
    """
    Configuration for the statistical analysis pipeline.

    Attributes:
        output_dir: Directory where analysis artifacts will be written.
        event_window_days: Number of days before/after each outage event
            included in the event study window.
        max_lag_days: Maximum lag in days for cross-correlation analysis.
        n_permutations: Number of permutations for empirical significance
            testing.
        random_seed: RNG seed used for reproducibility.
        regression_model: Which count model to fit.
        collapse_consecutive_outages: Whether consecutive outage days should
            be collapsed into a single event start.
    """

    output_dir: Path
    event_window_days: int = 7
    max_lag_days: int = 14
    n_permutations: int = 2_000
    random_seed: int = 42
    regression_model: RegressionModel = 'auto'
    collapse_consecutive_outages: bool = True


@dataclass(frozen=True)
class EventStudyResult:
    """
    Holds event-study outputs.

    Attributes:
        per_event: Long-form dataframe path containing per-event slices.
        summary: Summary dataframe path aggregated by relative day.
        observed_effect: Mean post-minus-pre effect across observed events.
        n_events_used: Number of events included after edge filtering.
    """

    per_event: Path
    summary: Path
    observed_effect: float
    n_events_used: int


@dataclass(frozen=True)
class CrossCorrelationResult:
    """
    Holds lagged cross-correlation outputs.

    Attributes:
        csv_path: CSV path with lag/correlation values.
        peak_lag_days: Lag with maximum absolute correlation.
        peak_correlation: Correlation at the peak lag.
    """

    csv_path: Path
    peak_lag_days: int
    peak_correlation: float


@dataclass(frozen=True)
class PermutationTestResult:
    """
    Holds permutation-test outputs.

    Attributes:
        csv_path: CSV containing the simulated null distribution.
        observed_effect: Observed event-study effect.
        p_value: Empirical two-sided p-value.
        n_permutations: Number of permutations run.
    """

    csv_path: Path
    observed_effect: float
    p_value: float
    n_permutations: int


@dataclass(frozen=True)
class RegressionResult:
    """
    Holds count-regression outputs.

    Attributes:
        summary_path: Path to the text summary.
        coefficients_path: Path to the coefficient table CSV.
        model_name: Name of the fitted model family.
    """

    summary_path: Path
    coefficients_path: Path
    model_name: str


@dataclass(frozen=True)
class AnalysisArtifacts:
    """
    Top-level analysis artifact bundle.

    Attributes:
        analysis_frame_csv: Daily merged frame used for all analysis.
        event_study: Event-study outputs.
        cross_correlation: Cross-correlation outputs.
        permutation_test: Permutation-test outputs.
        regression: Regression outputs.
        event_study_plot: PNG path for event-study figure.
        cross_correlation_plot: PNG path for lag-correlation figure.
        permutation_plot: PNG path for null-distribution figure.
    """

    analysis_frame_csv: Path
    event_study: EventStudyResult
    cross_correlation: CrossCorrelationResult
    permutation_test: PermutationTestResult
    regression: RegressionResult
    event_study_plot: Path
    cross_correlation_plot: Path
    permutation_plot: Path
