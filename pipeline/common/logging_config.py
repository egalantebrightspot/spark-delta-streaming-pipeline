"""Centralised logging configuration for the IoT telemetry pipeline.

Provides:
  - Consistent formatting across all pipeline layers
  - Per-layer log levels (bronze, silver, gold, generator, monitoring)
  - Rotating file handler for post-mortem debugging
  - Console handler for real-time operator visibility
  - One-time configuration driven by ``config.yaml``

Usage (unchanged from the original single-function API)::

    from pipeline.common.logging_config import get_logger
    logger = get_logger("bronze.ingest")
"""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

_LOG_FORMAT = "%(asctime)s | %(name)-30s | %(levelname)-7s | %(message)s"
_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

_configured = False
_log_cfg: dict = {}


def _resolve_level(level_str: str) -> int:
    """Map a case-insensitive level name to its ``logging`` constant."""
    return getattr(logging, level_str.upper(), logging.INFO)


def configure(config: dict = None) -> None:
    """One-time initialisation of the logging subsystem.

    Safe to call multiple times — only the first invocation takes effect.
    If *config* is ``None``, the pipeline config is loaded automatically.
    """
    global _configured, _log_cfg
    if _configured:
        return

    if config is None:
        from pipeline.common.utils import load_config
        config = load_config()

    _log_cfg = config.get("logging", {})

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT)

    # ── Console handler ─────────────────────────────────────────────────────
    console_cfg = _log_cfg.get("console", {})
    if console_cfg.get("enabled", True):
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(_resolve_level(console_cfg.get("level", "INFO")))
        console.setFormatter(formatter)
        root.addHandler(console)

    # ── Rotating file handler ───────────────────────────────────────────────
    file_cfg = _log_cfg.get("file", {})
    if file_cfg.get("enabled", True):
        log_dir = Path(_log_cfg.get("log_dir", "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            log_dir / "pipeline.log",
            maxBytes=int(_log_cfg.get("max_bytes", 10_485_760)),
            backupCount=int(_log_cfg.get("backup_count", 5)),
            encoding="utf-8",
        )
        file_handler.setLevel(_resolve_level(file_cfg.get("level", "DEBUG")))
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Quieten noisy third-party loggers that propagate through the root.
    for noisy in ("py4j", "py4j.java_gateway", "urllib3", "matplotlib"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Return a named logger with per-layer level from config.

    The *layer* is inferred from the first dotted segment of *name*
    (e.g. ``"bronze.ingest"`` → layer ``"bronze"``).  If no per-layer
    override exists in ``config.yaml``, the ``root_level`` is applied.

    An explicit *level* argument always takes priority over config.
    """
    configure()

    logger = logging.getLogger(name)

    if level is not None:
        logger.setLevel(level)
    else:
        layer = name.split(".")[0]
        layers = _log_cfg.get("layers", {})
        if layer in layers:
            logger.setLevel(_resolve_level(layers[layer]))
        else:
            logger.setLevel(_resolve_level(_log_cfg.get("root_level", "INFO")))

    return logger
