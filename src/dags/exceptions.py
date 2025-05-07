"""Custom exceptions for the dags library."""


class DagsError(Exception):
    """Base exception for all dags-specific errors."""


class AnnotationMismatchError(DagsError):
    """Raised when there is a mismatch between annotations."""


class MissingFunctionsError(DagsError):
    """Raised when required functions are missing from the DAG."""


class CyclicDependencyError(DagsError):
    """Raised when the DAG contains a cycle."""


class InvalidFunctionArgumentsError(DagsError):
    """Raised when there's an issue with function signatures or arguments."""


class ValidationError(DagsError):
    """Base exception for validation errors."""


class TrailingUnderscoreError(ValidationError):
    """Raised when path elements have trailing underscores."""


class RepeatedTopLevelElementError(ValidationError):
    """Raised when top-level elements are repeated in paths."""


class RepeatedElementInPathError(ValidationError):
    """Raised when elements are repeated in a single path."""
