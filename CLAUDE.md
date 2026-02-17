# CLAUDE.md

## Project Overview

**dags** is a Python library for creating executable DAGs (Directed Acyclic Graphs) from
interdependent functions. It provides tools to concatenate functions, manage type
annotations, and execute function graphs.

## Development Setup

This project uses **pixi** for environment management.

### Running Tests

```bash
pixi run -e py313 tests          # Run tests with Python 3.13
pixi run -e py314 tests          # Run tests with Python 3.14
pixi run -e py313 tests-with-cov # Run tests with coverage
```

Available Python environments: `py310`, `py311`, `py312`, `py313`, `py314`

### Type Checking

```bash
pixi run ty
```

### Linting & Formatting

```bash
pixi run prek run --all-files
```

### Building Docs

```bash
pixi run -e docs docs          # Build HTML docs with Jupyter Book
pixi run -e docs view-docs     # Live preview of docs
```

Documentation uses **Jupyter Book 2.0** with **MyST** markdown. Config is in
`docs/myst.yml`. Docs include executable Jupyter notebooks.

## Project Structure

```
src/dags/
├── __init__.py          # Main exports
├── annotations.py       # Type annotation handling
├── dag.py              # Core DAG functionality (concatenate_functions)
├── exceptions.py       # Custom exceptions
├── signature.py        # Function signature utilities
└── tree/               # Tree-related utilities

docs/
├── myst.yml             # Jupyter Book config
├── index.md             # Homepage
├── getting_started.md   # Getting started guide
├── usage_patterns.ipynb # Interactive examples notebook
└── tree.md              # Tree utilities docs

tests/
├── test_annotations.py  # Annotation tests
├── test_dag.py          # DAG concatenation tests
└── ...
```

## Key Modules

- **annotations.py**: Handles function type annotations, including a workaround for
  Python 3.14's `functools.wraps` annotation mismatch bug
- **dag.py**: Core `concatenate_functions()` for combining interdependent functions into
  a single callable
- **exceptions.py**: `AnnotationMismatchError`, `NonStringAnnotationError`, etc.

## Code Style

- Does **not** use `from __future__ import annotations` or `TYPE_CHECKING` blocks
- Ruff for linting (target: Python 3.11)
- ty for type checking (all rules set to error)
- NumPy docstring convention
- User-facing APIs accept `Sequence` (not `list`) for input parameters
