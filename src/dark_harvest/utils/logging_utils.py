from __future__ import annotations

import logging

from typing import Optional


def configure_logging(debug: bool = False, log_file: Optional[str] = None) -> None:
    """
    Configure application logging.

    Args:
        debug: If True, set DEBUG level. Otherwise INFO.
        log_file: Optional file path for logs; if None, logs go to stderr.
    """
    level = logging.DEBUG if debug else logging.INFO
    fmt = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'

    handlers: list[logging.Handler] = []
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    else:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(level=level, format=fmt, handlers=handlers)
    logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)
    logging.getLogger('PIL').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    if not debug:
        logging.getLogger('matplotlib').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
