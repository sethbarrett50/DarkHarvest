from __future__ import annotations

import datetime as dt

from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.dataclasses import dataclass

if TYPE_CHECKING:
    import argparse

BotnetMetric = Literal['records', 'sources', 'targets', 'tcp', 'udp']


@dataclass(
    config=ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
    )
)
class DarkHarvestConfig:
    """
    Validated runtime configuration for Dark Harvest.

    Attributes:
        start: Inclusive analysis start datetime.
        end: Inclusive analysis end datetime.
        ports: DShield ports used to build the botnet activity proxy.
        botnet_metric: DShield metric to aggregate across ports.
        user_agent: User-Agent header sent to remote APIs.
        debug: Whether debug logging is enabled.
    """

    start: dt.datetime
    end: dt.datetime
    ports: list[int] = Field(default_factory=lambda: [23, 2323, 7547, 5555])
    botnet_metric: BotnetMetric = 'sources'
    user_agent: str = 'outage-overlay-script (contact: you@example.com)'
    debug: bool = False

    @field_validator('start', 'end', mode='before')
    @classmethod
    def _parse_datetime(cls, value: object) -> dt.datetime:
        """
        Parse a datetime from an ISO-format string or pass through an
        existing datetime value.

        Args:
            value: Raw input value.

        Returns:
            Parsed datetime.

        Raises:
            ValueError: If the string is not valid ISO format.
            TypeError: If the input type is unsupported.
        """
        if isinstance(value, dt.datetime):
            return value

        if isinstance(value, str):
            try:
                return dt.datetime.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f'Invalid ISO date/datetime: {value!r}. Expected YYYY-MM-DD or a full ISO 8601 datetime string.'
                ) from exc

        raise TypeError('Expected a datetime or ISO-format string.')

    @field_validator('ports')
    @classmethod
    def _validate_ports(cls, value: list[int]) -> list[int]:
        """
        Validate that at least one valid TCP/UDP port is provided.

        Args:
            value: Input port list.

        Returns:
            Validated port list.

        Raises:
            ValueError: If the list is empty or contains invalid ports.
        """
        if not value:
            raise ValueError('At least one port must be provided.')

        for port in value:
            if not 1 <= port <= 65535:
                raise ValueError(f'Invalid port {port}. Must be in range 1-65535.')

        return value

    @field_validator('user_agent')
    @classmethod
    def _validate_user_agent(cls, value: str) -> str:
        """
        Validate that the User-Agent string is not empty.

        Args:
            value: Input User-Agent.

        Returns:
            Validated User-Agent string.

        Raises:
            ValueError: If the value is empty after stripping whitespace.
        """
        if not value.strip():
            raise ValueError('user_agent must not be empty.')
        return value

    @model_validator(mode='after')
    def _validate_date_range(self) -> 'DarkHarvestConfig':
        """
        Validate that the end datetime is not before the start datetime.

        Returns:
            The validated config instance.

        Raises:
            ValueError: If end is before start.
        """
        if self.end < self.start:
            raise ValueError('end must be greater than or equal to start.')
        return self

    @classmethod
    def from_namespace(cls, args: argparse.Namespace) -> 'DarkHarvestConfig':
        """
        Build a validated config from an argparse Namespace.

        Args:
            args: Parsed CLI arguments.

        Returns:
            Validated DarkHarvestConfig instance.
        """
        return cls(
            start=args.start,
            end=args.end,
            ports=list(args.ports),
            botnet_metric=args.botnet_metric,
            user_agent=args.user_agent,
            debug=bool(args.debug),
        )
