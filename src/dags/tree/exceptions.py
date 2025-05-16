"""Custom exceptions for the dags library."""

from __future__ import annotations

from dags.exceptions import ValidationError


class TrailingUnderscoreError(ValidationError):
    """Raised when path elements have trailing underscores."""


class RepeatedTopLevelElementError(ValidationError):
    """Raised when top-level elements are repeated in paths."""
