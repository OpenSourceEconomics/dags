@.ai-instructions/profiles/tier-a.md

# dags

## Overview

**dags** is a Python library for creating executable DAGs (Directed Acyclic Graphs) from
interdependent functions. It provides tools to concatenate functions, manage type
annotations, and execute function graphs.

## Build & Test

This project uses [pixi](https://pixi.sh) for environment management.

```bash
pixi run -e py314 tests          # Run tests with Python 3.14
pixi run -e py315 tests          # Run tests with Python 3.15
pixi run -e py314 tests-with-cov # Run tests with coverage
prek run ty --all-files          # Type checking (ty runs as a prek hook)
prek run --all-files             # Linting, formatting & type checking
pixi run -e docs build-docs      # Build HTML docs with Jupyter Book
pixi run -e docs view-docs       # Live preview of docs
```

Available Python environments: `py312`, `py313`, `py314`, `py315`

Documentation uses **Jupyter Book 2.0** with **MyST** markdown. Config is in
`docs/myst.yml`. Docs include executable Jupyter notebooks.

## Architecture

```
src/dags/
├── __init__.py          # Main exports
├── annotations.py       # Type annotation handling
├── dag.py               # Core DAG functionality (concatenate_functions)
├── exceptions.py        # Custom exceptions
├── output.py            # Output processing utilities
├── signature.py         # Function signature utilities
├── utils.py             # General utilities
└── tree/                # Tree-related utilities

docs/
├── myst.yml             # Jupyter Book config
├── index.ipynb          # Homepage
├── getting_started.ipynb # Getting started guide
├── usage_patterns.ipynb # Interactive examples notebook
├── tree.ipynb           # Tree utilities docs
└── api.md               # API reference

tests/
├── test_annotations.py  # Annotation tests
├── test_dag.py          # DAG concatenation tests
├── test_signature.py    # Signature tests
└── ...
```

### Key Modules

- **annotations.py**: Handles function type annotations, including a workaround for
  Python 3.14's `functools.wraps` annotation mismatch bug
- **dag.py**: Core `concatenate_functions()` for combining interdependent functions into
  a single callable
- **exceptions.py**: `AnnotationMismatchError`, `NonStringAnnotationError`, etc.

## Code Style

- Does **not** use `from __future__ import annotations` or `TYPE_CHECKING` blocks
- Ruff for linting (target: Python 3.12)
- ty for type checking (all rules set to error)
- Google docstring convention
- User-facing APIs accept `Sequence` (not `list`) for input parameters
