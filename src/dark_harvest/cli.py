from __future__ import annotations

import argparse
import logging

from pathlib import Path

from .analysis import AnalysisConfig, run_analysis_pipeline
from .plotting import plot_overlay
from .processing import build_botnet_proxy_series, incidents_to_df
from .sources.aws import fetch_aws_incidents
from .sources.cloudflare import fetch_cloudflare_incidents
from .sources.dshield import DshieldClient
from .sources.gcp import fetch_gcp_incidents
from .utils.config import DarkHarvestConfig
from .utils.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Overlay cloud outages with a botnet activity proxy time series.',
    )
    parser.add_argument('--start', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='YYYY-MM-DD')
    parser.add_argument(
        '--ports',
        nargs='+',
        type=int,
        default=[23, 2323, 7547, 5555],
        help='Ports to use for botnet proxy (default: 23 2323 7547 5555)',
    )
    parser.add_argument(
        '--botnet-metric',
        default='sources',
        choices=['records', 'sources', 'targets', 'tcp', 'udp'],
        help='DShield metric to sum across ports (default: sources)',
    )
    parser.add_argument(
        '--user-agent',
        default='outage-overlay-script (contact: you@example.com)',
        help='Custom User-Agent for DShield API requests (recommended)',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging.',
    )
    parser.add_argument(
        '--run-analysis',
        action='store_true',
        help='Run statistical analysis after data collection.',
    )
    parser.add_argument(
        '--event-window',
        type=int,
        default=7,
        help='Days before/after outage starts for event study (default: 7).',
    )
    parser.add_argument(
        '--max-lag',
        type=int,
        default=14,
        help='Maximum lag in days for cross-correlation (default: 14).',
    )
    parser.add_argument(
        '--n-permutations',
        type=int,
        default=2000,
        help='Number of permutations for empirical significance (default: 2000).',
    )
    parser.add_argument(
        '--random-seed',
        type=int,
        default=42,
        help='RNG seed for reproducible permutation testing (default: 42).',
    )
    parser.add_argument(
        '--regression-model',
        choices=['auto', 'poisson', 'negbin'],
        default='auto',
        help='Count regression model to fit (default: auto).',
    )
    return parser.parse_args()


def _build_output_dir(config: DarkHarvestConfig) -> Path:
    """
    Build an output directory for one configuration combination.

    Args:
        config: Parsed runtime config.

    Returns:
        Output directory path.
    """
    port_text = '-'.join(str(port) for port in config.ports)
    dir_name = f'{config.start:%Y%m%d}_{config.end:%Y%m%d}__metric-{config.botnet_metric}__ports-{port_text}'
    return Path('output') / dir_name


def main() -> None:
    """
    CLI entrypoint to build outage timetable, overlay plots, and optionally
    run the statistical analysis pipeline.
    """
    args = _parse_args()
    config = DarkHarvestConfig.from_namespace(args)
    configure_logging(debug=bool(args.debug))

    start = config.start
    end = config.end
    output_dir = _build_output_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)

    outages_csv = output_dir / 'outages.csv'
    overlay_png = output_dir / 'overlay.png'

    logger.info('Starting run: start=%s end=%s', start.date(), end.date())
    logger.info(
        'Botnet proxy config: ports=%s metric=%s',
        config.ports,
        config.botnet_metric,
    )
    logger.info('Output directory: %s', output_dir)
    logger.debug('User-Agent: %s', config.user_agent)

    aws = fetch_aws_incidents(start, end)
    logger.debug('AWS incidents pulled')
    gcp = fetch_gcp_incidents(start, end)
    logger.debug('GCP incidents pulled')
    cloudflare = fetch_cloudflare_incidents(start, end)
    logger.debug('Cloudflare incidents pulled')

    logger.info(
        'Fetched incidents: AWS=%d GCP=%d Cloudflare=%d',
        len(aws),
        len(gcp),
        len(cloudflare),
    )

    outages_df = incidents_to_df([*aws, *gcp, *cloudflare])
    logger.info('Outages dataframe rows: %d', len(outages_df))
    logger.debug('Outages df head:\n%s', outages_df.head(10).to_string(index=False))

    outages_df.to_csv(outages_csv, index=False)

    dshield = DshieldClient(user_agent=config.user_agent)
    botnet_daily = build_botnet_proxy_series(
        client=dshield,
        ports=config.ports,
        start=start,
        end=end,
        metric=str(config.botnet_metric),
    )

    logger.info('Botnet daily series rows: %d', len(botnet_daily))
    logger.debug(
        'Botnet daily head:\n%s',
        botnet_daily.head(10).to_string(index=False),
    )

    plot_overlay(outages_df, botnet_daily, start, end, overlay_png)

    logger.info('Wrote outage timetable: %s', outages_csv)
    logger.info('Wrote overlay plot: %s', overlay_png)

    if args.run_analysis:
        analysis_config = AnalysisConfig(
            output_dir=output_dir,
            event_window_days=args.event_window,
            max_lag_days=args.max_lag,
            n_permutations=args.n_permutations,
            random_seed=args.random_seed,
            regression_model=args.regression_model,
            collapse_consecutive_outages=True,
        )

        artifacts = run_analysis_pipeline(
            outages_df=outages_df,
            botnet_daily=botnet_daily,
            start=start,
            end=end,
            config=analysis_config,
        )

        logger.info('Analysis frame: %s', artifacts.analysis_frame_csv)
        logger.info('Event study summary: %s', artifacts.event_study.summary)
        logger.info(
            'Permutation p-value: %.6f',
            artifacts.permutation_test.p_value,
        )
        logger.info(
            'Peak lag correlation: lag=%d corr=%.4f',
            artifacts.cross_correlation.peak_lag_days,
            artifacts.cross_correlation.peak_correlation,
        )
        logger.info(
            'Regression model used: %s',
            artifacts.regression.model_name,
        )


if __name__ == '__main__':
    main()
