"""
Colorized, dual-sink (console + file) logger factory.
Designed for live SSH monitoring with millisecond timestamps.
"""
import logging
import sys
from pathlib import Path
from config.settings import Settings

try:
    import colorlog
    _HAS_COLOR = True
except ImportError:
    _HAS_COLOR = False

_CONFIGURED: set[str] = set()

def get_logger(name: str) -> logging.Logger:
    if name in _CONFIGURED:
        return logging.getLogger(name)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, Settings.LOG_LEVEL, logging.DEBUG))
    logger.propagate = False

    # ── Console ──────────────────────────────────────────────
    if _HAS_COLOR:
        cfmt = colorlog.ColoredFormatter(
            fmt=(
                "%(log_color)s%(asctime)s.%(msecs)03d │ %(levelname)-8s │ "
                "%(name)-20s │ %(message)s%(reset)s"
            ),
            datefmt="%H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    else:
        cfmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d │ %(levelname)-8s │ %(name)-20s │ %(message)s",
            datefmt="%H:%M:%S",
        )

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(cfmt)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    # ── File ─────────────────────────────────────────────────
    log_path = Path(Settings.LOG_FILE)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ffmt = logging.Formatter(
        "%(asctime)s │ %(levelname)-8s │ %(name)-20s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setFormatter(ffmt)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    _CONFIGURED.add(name)
    return logger
