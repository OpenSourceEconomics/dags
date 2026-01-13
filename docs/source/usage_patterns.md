# Usage Patterns

This guide shows common patterns for using dags, based on real-world usage in projects like [pylcm](https://github.com/OpenSourceEconomics/pylcm) and [ttsim](https://github.com/ttsim-dev/ttsim).

## Pattern 1: Function Composition Pipeline

The most common pattern is combining multiple interdependent functions into a single computation:

```python
import dags

def raw_data():
    return [1, 2, 3, 4, 5]

def cleaned_data(raw_data):
    return [x for x in raw_data if x > 0]

def statistics(cleaned_data):
    return {
        "mean": sum(cleaned_data) / len(cleaned_data),
        "count": len(cleaned_data),
    }

def report(statistics, cleaned_data):
    return f"Processed {statistics['count']} items, mean: {statistics['mean']}"

# Create pipeline
pipeline = dags.concatenate_functions(
    functions={
        "raw_data": raw_data,
        "cleaned_data": cleaned_data,
        "statistics": statistics,
        "report": report,
    },
    targets=["report"],
    return_type="dict",
)

result = pipeline()
```

## Pattern 2: Parameterized Computations

Pass external parameters to the combined function:

```python
def utility(consumption, risk_aversion):
    """CRRA utility function."""
    if risk_aversion == 1:
        return np.log(consumption)
    return (consumption ** (1 - risk_aversion)) / (1 - risk_aversion)

def budget_constraint(income, price):
    return income / price

def optimal_consumption(budget_constraint):
    return budget_constraint  # simplified

combined = dags.concatenate_functions(
    functions={
        "utility": utility,
        "budget_constraint": budget_constraint,
        "optimal_consumption": optimal_consumption,
    },
    targets=["utility"],
)

# External inputs: income, price, consumption, risk_aversion
result = combined(income=1000, price=10, consumption=50, risk_aversion=2)
```

## Pattern 3: Aggregating Multiple Functions

Use an aggregator to combine outputs from multiple functions:

```python
import numpy as np

def constraint_1(x):
    return x > 0

def constraint_2(x, y):
    return x + y < 100

def constraint_3(y):
    return y >= 0

# Combine constraints with logical AND
all_constraints = dags.concatenate_functions(
    functions={
        "constraint_1": constraint_1,
        "constraint_2": constraint_2,
        "constraint_3": constraint_3,
    },
    targets=["constraint_1", "constraint_2", "constraint_3"],
    aggregator=np.logical_and,  # Combines all outputs
    aggregator_return_type=bool,
)

is_feasible = all_constraints(x=10, y=20)  # Returns True
```

## Pattern 4: Dynamic Function Generation

Generate functions dynamically and rename their arguments:

```python
def create_processor(scale_factor):
    """Factory function that creates a scaled processor."""
    def processor(input_value):
        return input_value * scale_factor
    return processor

# Create processors with different scales
processors = {
    f"scale_{i}": dags.rename_arguments(
        create_processor(i),
        mapper={"input_value": "data"}
    )
    for i in [1, 2, 5, 10]
}

def combine_scaled(scale_1, scale_2, scale_5, scale_10):
    return scale_1 + scale_2 + scale_5 + scale_10

processors["combine_scaled"] = combine_scaled

combined = dags.concatenate_functions(
    functions=processors,
    targets=["combine_scaled"],
)

result = combined(data=100)  # 100 + 200 + 500 + 1000 = 1800
```

## Pattern 5: Selective Target Computation

Compute only specific outputs by specifying targets:

```python
def expensive_computation(data):
    # ... costly operation
    return result

def cheap_summary(data):
    return len(data)

def full_report(expensive_computation, cheap_summary):
    return {"full": expensive_computation, "summary": cheap_summary}

functions = {
    "expensive_computation": expensive_computation,
    "cheap_summary": cheap_summary,
    "full_report": full_report,
}

# Only compute what's needed for cheap_summary
quick = dags.concatenate_functions(
    functions=functions,
    targets=["cheap_summary"],  # expensive_computation won't run
)

# Compute everything
full = dags.concatenate_functions(
    functions=functions,
    targets=["full_report"],
)
```

## Pattern 6: Dependency Analysis

Analyze which functions affect specific outputs:

```python
def find_influential_variables(functions, target):
    """Find all variables that influence a target."""
    ancestors = dags.get_ancestors(
        functions=functions,
        targets=[target],
        include_targets=False,
    )
    return ancestors

# Find what affects 'net_income'
influential = find_influential_variables(functions, "net_income")
# Returns set of all upstream function names
```

## Pattern 7: Working with Nested Structures

Use `dags.tree` for hierarchical function organization:

```python
import dags.tree as dt

# Nested function structure
functions = {
    "household": {
        "income": lambda: 50000,
        "expenses": lambda: 30000,
    },
    "taxes": {
        "federal": lambda household__income: household__income * 0.2,
        "state": lambda household__income: household__income * 0.05,
    },
}

# Flatten to qualified names for processing
flat_functions = dt.flatten_to_qnames(functions)
# {"household__income": ..., "household__expenses": ..., "taxes__federal": ..., ...}

# Use with concatenate_functions
combined = dags.concatenate_functions(
    functions=flat_functions,
    targets=["taxes__federal", "taxes__state"],
)
```

See the [Tree documentation](tree.md) for more details.

## Pattern 8: Signature Inspection and Modification

Inspect and modify function signatures for integration:

```python
from dags.signature import with_signature

# Get original arguments
original_args = dags.get_free_arguments(some_function)

# Create a wrapper with a new signature
@with_signature(
    args=["new_param_1", "new_param_2"],
    return_annotation="float",
)
def wrapper(**kwargs):
    return some_function(kwargs["new_param_1"], kwargs["new_param_2"])
```

## Best Practices

1. **Use descriptive function names**: Since dags uses names for dependency resolution, clear names make the DAG easier to understand.

2. **Keep functions focused**: Each function should do one thing well, making the DAG modular and testable.

3. **Document dependencies**: Even though dags infers dependencies, documenting expected inputs helps maintainability.

4. **Use `enforce_signature=False` for dynamic cases**: When functions have dynamic signatures, disable signature enforcement:

   ```python
   combined = dags.concatenate_functions(
       functions=functions,
       targets=targets,
       enforce_signature=False,
   )
   ```

5. **Set annotations for type checking**: Enable type annotations on the combined function:

   ```python
   combined = dags.concatenate_functions(
       functions=functions,
       targets=targets,
       set_annotations=True,
   )
   ```
