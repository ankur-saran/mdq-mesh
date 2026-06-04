"""Structured logging factory for mdq-mesh (NFR-8)."""

from __future__ import annotations

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a named logger under the mdq hierarchy."""
    logger = logging.getLogger(f"mdq.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def configure_root(level: str = "INFO") -> None:
    """Set the verbosity for all mdq loggers. Call once at process startup."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("mdq").setLevel(log_level)
