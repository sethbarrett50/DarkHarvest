from __future__ import annotations

import argparse
import datetime as dt
import logging

from src.plotting import plot_overlay
from src.processing import build_botnet_proxy_series, incidents_to_df
from src.sources.aws import fetch_aws_incidents
from src.sources.cloudflare import fetch_cloudflare_incidents
from src.sources.dshield import DshieldClient
from src.sources.gcp import fetch_gcp_incidents
from src.utils.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Overlay cloud outages with a botnet activity proxy time series.')
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
    parser.add_argument('--out-csv', default='outages.csv')
    parser.add_argument('--out-plot', default='overlay.png')
    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint to build outage timetable + botnet proxy overlay plot.
    """
    args = _parse_args()
    configure_logging(debug=bool(args.debug))

    start = dt.datetime.fromisoformat(args.start)
    end = dt.datetime.fromisoformat(args.end)

    logger.info('Starting run: start=%s end=%s', start.date(), end.date())
    logger.info(
        'Botnet proxy config: ports=%s metric=%s',
        list(args.ports),
        args.botnet_metric,
    )
    logger.debug('User-Agent: %s', args.user_agent)

    aws = fetch_aws_incidents(start, end)
    gcp = fetch_gcp_incidents(start, end)
    cloudflare = fetch_cloudflare_incidents(start, end)

    logger.info(
        'Fetched incidents: AWS=%d GCP=%d Cloudflare=%d',
        len(aws),
        len(gcp),
        len(cloudflare),
    )

    outages_df = incidents_to_df([*aws, *gcp, *cloudflare])

    logger.info('Outages dataframe rows: %d', len(outages_df))
    logger.debug('Outages df head:\n%s', outages_df.head(10).to_string(index=False))

    outages_df.to_csv(args.out_csv, index=False)

    dshield = DshieldClient(user_agent=args.user_agent)
    botnet_daily = build_botnet_proxy_series(
        client=dshield,
        ports=list(args.ports),
        start=start,
        end=end,
        metric=str(args.botnet_metric),
    )

    logger.info('Botnet daily series rows: %d', len(botnet_daily))
    logger.debug('Botnet daily head:\n%s', botnet_daily.head(10).to_string(index=False))

    plot_overlay(outages_df, botnet_daily, start, end, args.out_plot)

    logger.info('Wrote outage timetable: %s', args.out_csv)
    logger.info('Wrote overlay plot: %s', args.out_plot)


if __name__ == '__main__':
    main()
