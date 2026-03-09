from __future__ import annotations

import datetime as dt

from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.dataclasses import dataclass

if TYPE_CHECKING:
    import argparse

BotnetMetric = Literal['records', 'sources', 'targets', 'tcp', 'udp']


@dataclass(config=ConfigDict(
    frozen=True,
    str_strip_whitespace=True,
))
class DarkHarvestConfig:
    """
    Validated runtime configuration for Dark Harvest.
    """

    start: dt.datetime
    end: dt.datetime
    ports: list[int] = Field(default_factory=lambda: [23, 2323, 7547, 5555])
    botnet_metric: BotnetMetric = 'sources'
    user_agent: str = 'outage-overlay-script (contact: you@example.com)'
    debug: bool = False
    out_csv: Path = Path('outages.csv')
    out_plot: Path = Path('overlay.png')

    @field_validator('start', 'end', mode='before')
    @classmethod
    def _parse_datetime(cls, value: object) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value

        if isinstance(value, str):
            try:
                return dt.datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f'Invalid ISO date/datetime: {value!r}. Expected YYYY-MM-DD '
                    'or a full ISO 8601 datetime string.'
                ) from exc

        raise TypeError('Expected a datetime or ISO-format string.')

    @field_validator('ports')
    @classmethod
    def _validate_ports(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError('At least one port must be provided.')

        for port in value:
            if not 1 <= port <= 65535:
                raise ValueError(
                    f'Invalid port {port}. Must be in range 1-65535.')

        return value

    @field_validator('user_agent')
    @classmethod
    def _validate_user_agent(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('user_agent must not be empty.')
        return value

    @model_validator(mode='after')
    def _validate_date_range(self) -> 'DarkHarvestConfig':
        if self.end < self.start:
            raise ValueError('end must be greater than or equal to start.')
        return self

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> 'DarkHarvestConfig':
        """
        Build validated config from argparse Namespace.
        """
        return cls(
            start=args.start,
            end=args.end,
            ports=list(args.ports),
            botnet_metric=args.botnet_metric,
            user_agent=args.user_agent,
            debug=bool(args.debug),
            out_csv=args.out_csv,
            out_plot=args.out_plot,
        )
