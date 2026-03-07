"""Shared utility functions for dags."""

import textwrap
from collections.abc import Sequence


def format_list_linewise(seq: Sequence[object]) -> str:
    """Format a sequence as a multi-line list string for error messages."""
    formatted_list = '",\n    "'.join([str(c) for c in seq])
    return textwrap.dedent(
        """
        [
            "{formatted_list}",
        ]
        """,
    ).format(formatted_list=formatted_list)
