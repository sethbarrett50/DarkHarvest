from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime as dt


@dataclass(frozen=True)
class Incident:
    """
    Represents a provider outage/incident window.

    Attributes:
        provider: Provider name (e.g., AWS, GCP, Cloudflare).
        incident_id: Provider-specific incident identifier.
        title: Human-readable incident summary/title.
        start: Incident start timestamp (UTC-naive).
        end: Incident end timestamp (UTC-naive).
        severity: Provider-specific severity/impact label if available.
        url: Provider status URL for the incident.
    """

    provider: str
    incident_id: str
    title: str
    start: dt.datetime
    end: dt.datetime
    severity: str = ''
    url: str = ''
