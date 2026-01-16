# Usage Patterns

This guide shows common patterns for using dags, based on real-world usage in projects like [pylcm](https://github.com/OpenSourceEconomics/pylcm) and [ttsim](https://github.com/ttsim-dev/ttsim).

## Pattern 1: Building Computational Pipelines

The core use case for dags is combining multiple interdependent functions into a single
callable. This is powerful because **the same set of functions can be combined in
different ways** depending on what you want to compute.

### Example: Data Processing Pipeline

Here's a simple data processing pipeline where raw data flows through cleaning,
statistics computation, and report generation:

```python
import dags

def cleaned_data(raw_data):
    return [x for x in raw_data if x > 0]

def statistics(cleaned_data):
    return {
        "mean": sum(cleaned_data) / len(cleaned_data),
        "count": len(cleaned_data),
    }

def report(statistics, cleaned_data):
    return f"Processed {statistics['count']} items, mean: {statistics['mean']}"

functions = {
    "cleaned_data": cleaned_data,
    "statistics": statistics,
    "report": report,
}

# Create the full pipeline
pipeline = dags.concatenate_functions(
    functions=functions,
    targets=["report"],
    return_type="dict",
)

# raw_data is an external input (not computed by any function)
result = pipeline(raw_data=[1, -2, 3, 4, -5, 6])
# result = {"report": "Processed 4 items, mean: 3.5"}
```

### Example: Economic Model with Utility Maximization

Consider a consumer choosing consumption to maximize utility subject to a budget
constraint. We define the model components as separate functions:

```python
import numpy as np
import dags

def utility(consumption, risk_aversion):
    """CRRA utility function."""
    if risk_aversion == 1:
        return np.log(consumption)
    return (consumption ** (1 - risk_aversion)) / (1 - risk_aversion)

def budget_constraint(income, price):
    """Maximum affordable consumption."""
    return income / price

def feasible(consumption, budget_constraint):
    """Check if consumption is affordable."""
    return consumption <= budget_constraint

def optimal_utility(budget_constraint, risk_aversion):
    """Find maximum utility over a grid of consumption values."""
    consumption_grid = np.linspace(0.1, budget_constraint, 100)
    if risk_aversion == 1:
        utilities = np.log(consumption_grid)
    else:
        utilities = (consumption_grid ** (1 - risk_aversion)) / (1 - risk_aversion)
    return float(np.max(utilities))

functions = {
    "utility": utility,
    "budget_constraint": budget_constraint,
    "feasible": feasible,
    "optimal_utility": optimal_utility,
}
```

Now the power of dags becomes clear: **we can create different combined functions from
the same building blocks** depending on what we need:

```python
# 1. Compute optimal utility given income and prices
solve_model = dags.concatenate_functions(
    functions=functions,
    targets=["optimal_utility"],
    return_type="dict",
)
result = solve_model(income=1000, price=10, risk_aversion=2)
# result = {"optimal_utility": -0.01}

# 2. Evaluate utility and check feasibility for a specific consumption choice
evaluate_choice = dags.concatenate_functions(
    functions=functions,
    targets=["utility", "feasible"],
    return_type="dict",
)
result = evaluate_choice(
    income=1000, price=10, consumption=50, risk_aversion=2
)
# result = {"utility": -0.02, "feasible": True}

# 3. Just compute the budget constraint
get_budget = dags.concatenate_functions(
    functions=functions,
    targets=["budget_constraint"],
    return_type="dict",
)
result = get_budget(income=1000, price=10)
# result = {"budget_constraint": 100.0}
```

This pattern is particularly useful when:
- You have a complex model with many interrelated components
- Different use cases require computing different subsets of outputs
- You want to avoid code duplication by reusing the same function definitions
- The computation graph may change based on user configuration

## Pattern 2: Aggregating Multiple Functions

When you have multiple functions that should be combined into a single result, use an
aggregator. This is common when checking multiple constraints or combining scores.

**When to use this pattern:**
- Checking if multiple constraints are all satisfied
- Combining multiple penalty terms or objective function components
- Voting or ensemble methods where multiple models contribute to a decision

```python
import numpy as np
import dags

def positive_consumption(consumption):
    """Consumption must be positive."""
    return consumption > 0

def within_budget(consumption, budget_constraint):
    """Consumption must not exceed budget."""
    return consumption <= budget_constraint

def minimum_savings(consumption, income):
    """Must save at least 10% of income."""
    return consumption <= 0.9 * income

# Combine all constraints with logical AND
all_feasible = dags.concatenate_functions(
    functions={
        "positive_consumption": positive_consumption,
        "within_budget": within_budget,
        "minimum_savings": minimum_savings,
    },
    targets=["positive_consumption", "within_budget", "minimum_savings"],
    aggregator=np.logical_and,
    aggregator_return_type=bool,
)

# Check if a consumption choice satisfies all constraints
is_ok = all_feasible(consumption=80, budget_constraint=100, income=100)
# Returns True (80 > 0, 80 <= 100, 80 <= 90)

is_ok = all_feasible(consumption=95, budget_constraint=100, income=100)
# Returns False (95 > 90 violates minimum_savings)
```

## Pattern 3: Generating Functions for Multiple Scenarios

In economic modeling, you often need to create similar functions for different scenarios,
time periods, or agent types. Rather than writing each function by hand, you can generate
them programmatically and use `rename_arguments` to ensure they connect properly in the
DAG.

**When to use this pattern:**
- Creating period-specific functions in a dynamic model (e.g., different tax rules by year)
- Generating agent-type-specific behavior (e.g., different utility functions by household type)
- Building functions for multiple regions or sectors with the same structure

```python
import dags

def create_income_tax(rate, threshold):
    """Create a tax function with given rate and threshold."""
    def income_tax(gross_income):
        taxable = max(0, gross_income - threshold)
        return taxable * rate
    return income_tax

# Tax rules changed over time
tax_rules = {
    2020: {"rate": 0.25, "threshold": 10000},
    2021: {"rate": 0.27, "threshold": 12000},
    2022: {"rate": 0.30, "threshold": 12000},
}

# Generate tax functions for each year
functions = {}
for year, params in tax_rules.items():
    tax_func = create_income_tax(params["rate"], params["threshold"])
    # Rename so each function takes year-specific income
    functions[f"tax_{year}"] = dags.rename_arguments(
        tax_func,
        mapper={"gross_income": f"income_{year}"}
    )

def total_tax_burden(tax_2020, tax_2021, tax_2022):
    """Sum of taxes across all years."""
    return tax_2020 + tax_2021 + tax_2022

functions["total_tax_burden"] = total_tax_burden

combined = dags.concatenate_functions(
    functions=functions,
    targets=["total_tax_burden"],
    return_type="dict",
)

# Compute total taxes given income trajectory
result = combined(income_2020=50000, income_2021=55000, income_2022=60000)
# result = {"total_tax_burden": 36010.0}
```

## Pattern 4: Selective Computation

When your function graph contains expensive computations, you can create multiple
combined functions that compute only what's needed. dags automatically prunes the
computation graph to include only the functions required for the specified targets.

**When to use this pattern:**
- Some outputs are expensive to compute and not always needed
- You want fast feedback during development by computing only key outputs
- Different analyses or reports need different subsets of results

```python
import dags

def simulated_data(parameters, n_simulations):
    """Expensive Monte Carlo simulation."""
    # ... costly operation that takes minutes
    return simulated_results

def summary_statistics(simulated_data):
    """Compute mean, std, etc. from simulations."""
    return {"mean": ..., "std": ...}

def full_distribution(simulated_data):
    """Compute full empirical distribution."""
    return distribution

def quick_check(parameters):
    """Fast sanity check of parameters."""
    return all(p > 0 for p in parameters.values())

functions = {
    "simulated_data": simulated_data,
    "summary_statistics": summary_statistics,
    "full_distribution": full_distribution,
    "quick_check": quick_check,
}

# For quick validation: only runs quick_check, skips simulation
validator = dags.concatenate_functions(
    functions=functions,
    targets=["quick_check"],
)

# For summary results: runs simulation + summary_statistics
summarizer = dags.concatenate_functions(
    functions=functions,
    targets=["summary_statistics"],
)

# For full analysis: runs everything
full_analysis = dags.concatenate_functions(
    functions=functions,
    targets=["summary_statistics", "full_distribution"],
)
```

## Pattern 5: Dependency Analysis

Use `get_ancestors` to analyze which inputs affect specific outputs. This is useful for
understanding model structure, debugging, and optimizing computations.

**When to use this pattern:**
- Understanding which parameters affect a specific output
- Identifying the minimal set of inputs needed for a computation
- Debugging unexpected results by tracing dependencies

```python
import dags

def wage(education, experience):
    return 20000 + 5000 * education + 1000 * experience

def capital_income(wealth, interest_rate):
    return wealth * interest_rate

def total_income(wage, capital_income):
    return wage + capital_income

def consumption(total_income, savings_rate):
    return total_income * (1 - savings_rate)

functions = {
    "wage": wage,
    "capital_income": capital_income,
    "total_income": total_income,
    "consumption": consumption,
}

# What affects consumption? (includes both functions and their inputs)
ancestors = dags.get_ancestors(
    functions=functions,
    targets=["consumption"],
    include_targets=True,
)
# Returns all nodes in the dependency graph:
# {"wage", "capital_income", "total_income", "consumption",
#  "education", "experience", "wealth", "interest_rate", "savings_rate"}

# What are the external inputs (leaf nodes)?
all_args = set()
for func in functions.values():
    all_args.update(dags.get_free_arguments(func))
external_inputs = all_args - set(functions.keys())
# Returns: {"education", "experience", "wealth", "interest_rate", "savings_rate"}
```

## Pattern 6: Working with Nested Structures

Use `dags.tree` for hierarchical function organization. This is useful when you have
functions grouped by category, region, time period, or any other hierarchy.

**When to use this pattern:**
- Organizing functions by logical groups (e.g., taxes, transfers, labor market)
- Working with multi-region or multi-sector models
- Keeping namespaces separate to avoid naming conflicts

```python
import dags
import dags.tree as dt

# Nested function structure representing a tax-transfer system
functions = {
    "income": {
        "wage": lambda hours, hourly_wage: hours * hourly_wage,
        "capital": lambda wealth, interest_rate: wealth * interest_rate,
    },
    "taxes": {
        "income_tax": lambda income__wage, income__capital: (
            0.3 * (income__wage + income__capital)
        ),
    },
    "transfers": {
        "basic_income": lambda: 500,
    },
    "net_income": lambda income__wage, income__capital, taxes__income_tax, transfers__basic_income: (
        income__wage + income__capital - taxes__income_tax + transfers__basic_income
    ),
}

# Flatten to qualified names for use with dags
flat_functions = dt.flatten_to_qnames(functions)

combined = dags.concatenate_functions(
    functions=flat_functions,
    targets=["net_income"],
    return_type="dict",
)

result = combined(hours=40, hourly_wage=25, wealth=10000, interest_rate=0.05)
# result = {"net_income": 1550.0}
```

See the [Tree documentation](tree.md) for more details.

## Pattern 7: Signature Inspection and Modification

Sometimes you need to inspect or modify function signatures, especially when integrating
functions from different sources or creating wrappers.

**When to use this pattern:**
- Integrating functions from external libraries with different naming conventions
- Creating generic wrappers that work with varying function signatures
- Building function registries or plugin systems

```python
import dags
from dags.signature import with_signature

# Inspect a function's arguments
def model(alpha, beta, gamma):
    return alpha + beta * gamma

args = dags.get_free_arguments(model)
# args = ["alpha", "beta", "gamma"]

# Rename arguments to match your naming convention
renamed = dags.rename_arguments(model, mapper={
    "alpha": "intercept",
    "beta": "slope",
    "gamma": "x",
})

# Verify the new signature
new_args = dags.get_free_arguments(renamed)
# new_args = ["intercept", "slope", "x"]

# Get type annotations (returns type objects, not strings)
def typed_func(x: float, y: int) -> float:
    return x + y

annotations = dags.get_annotations(typed_func)
# annotations = {"x": float, "y": int, "return": float}
```

## Best Practices

1. **Use descriptive function names**: Since dags uses names for dependency resolution,
   clear names make the DAG easier to understand and debug.

2. **Keep functions focused**: Each function should do one thing well, making the DAG
   modular and testable. This also makes it easier to compute different subsets of
   outputs.

3. **Document dependencies**: Even though dags infers dependencies from parameter names,
   documenting expected inputs in docstrings helps maintainability.

4. **Use `enforce_signature=False` for dynamic cases**: When functions have dynamic
   signatures (e.g., generated at runtime), disable signature enforcement:

   ```python
   combined = dags.concatenate_functions(
       functions=functions,
       targets=targets,
       enforce_signature=False,
   )
   ```

5. **Set annotations for type checking**: Enable type annotations on the combined
   function for better IDE support and type checking:

   ```python
   combined = dags.concatenate_functions(
       functions=functions,
       targets=targets,
       set_annotations=True,
   )
   ```
