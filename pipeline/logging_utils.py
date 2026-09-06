"""Structured JSON logging.

Every task logs one JSON object per line so Airflow task logs stay greppable
and can be shipped to any log backend without a parser change.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if extra := getattr(record, "extra_fields", None):
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, msg: str, **fields) -> None:
    """Log a message with arbitrary structured fields attached."""
    logger.info(msg, extra={"extra_fields": fields})


@contextmanager
def timed(logger: logging.Logger, stage: str, **fields):
    """Time a pipeline stage and emit start/end events with duration."""
    start = time.perf_counter()
    log_event(logger, f"{stage}.start", stage=stage, **fields)
    try:
        yield
    except Exception as exc:
        log_event(
            logger, f"{stage}.failed", stage=stage,
            duration_s=round(time.perf_counter() - start, 3),
            error=str(exc), error_type=type(exc).__name__, **fields,
        )
        raise
    log_event(
        logger, f"{stage}.done", stage=stage,
        duration_s=round(time.perf_counter() - start, 3), **fields,
    )
