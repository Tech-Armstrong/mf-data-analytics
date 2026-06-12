"""
Centralised logger for the SIF pipeline. Import get_logger() in every script.
Mirror of config/logging_utils.py, writing to the SIF log file.
"""

import logging
import sys

from sif.config.constants import LOG_DIR, LOG_FILE, LOG_LEVEL


def get_logger(name: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"sif.{name}")
    if logger.handlers:           # avoid duplicate handlers on re-import
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # console — force UTF-8 so non-ASCII (arrows, box chars) never crash on
    # a cp1252 Windows console.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # file (always UTF-8)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
