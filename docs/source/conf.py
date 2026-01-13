"""Sphinx configuration for dags documentation."""

from importlib.metadata import version as get_version

# -- Project information -----------------------------------------------------

project = "dags"
author = "Janoś Gabler, Tobias Raabe"
copyright = f"2022, {author}"  # noqa: A001
release = get_version("dags")
version = ".".join(release.split(".")[:2])

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_design",
]

# MyST configuration
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# Source file configuration
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
master_doc = "index"
language = "en"
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**.ipynb_checkpoints"]

# Autodoc configuration
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autosummary_generate = True

# sphinx_autodoc_typehints configuration
always_use_bars_union = True
typehints_defaults = "comma"

# Suppress specific warnings
suppress_warnings = [
    "sphinx_autodoc_typehints.forward_reference",
    "sphinx_autodoc_typehints.guarded_import",
]

# Napoleon configuration (for Google/NumPy style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_use_param = True
napoleon_use_rtype = True

# Intersphinx configuration
intersphinx_mapping = {
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "python": ("https://docs.python.org/3.12", None),
    "networkx": ("https://networkx.org/documentation/stable", None),
}

# Copybutton configuration
copybutton_prompt_text = r"\$ |>>> |In \[\d\]: "
copybutton_prompt_is_regexp = True

# -- Options for HTML output -------------------------------------------------

html_theme = "pydata_sphinx_theme"
html_logo = "_static/images/logo.svg"
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]

html_theme_options = {
    "github_url": "https://github.com/OpenSourceEconomics/dags",
    "show_toc_level": 2,
    "show_prev_next": True,
    "navigation_with_keys": True,
}

html_sidebars = {
    "**": [
        "sidebar-nav-bs.html",
    ],
}
