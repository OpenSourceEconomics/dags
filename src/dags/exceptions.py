"""Custom exceptions for the dags library."""

from __future__ import annotations


class DagsError(Exception):
    """Base exception for all dags-specific errors."""


class AnnotationMismatchError(DagsError):
    """Raised when there is a mismatch between annotations."""


class NonStringAnnotationError(DagsError):
    """Raised when a non-string annotation is encountered."""


class MissingFunctionsError(DagsError):
    """Raised when required functions are missing from the DAG."""


class CyclicDependencyError(DagsError):
    """Raised when the DAG contains a cycle."""


class InvalidFunctionArgumentsError(DagsError):
    """Raised when there's an issue with function signatures or arguments."""


class ValidationError(DagsError):
    """Base exception for validation errors."""
