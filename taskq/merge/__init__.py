"""Standalone FIFO merge workflow public API."""

from .config import build_merge_spec
from .workflow import cancel_merge_job, register_merge_job

__all__ = [
    'build_merge_spec',
    'cancel_merge_job',
    'register_merge_job',
]

