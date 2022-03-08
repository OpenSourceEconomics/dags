import os
from importlib.metadata import version


author = "Janoś Gabler, Tobias Raabe"

# Set variable so that todos are shown in local build
on_rtd = os.environ.get("READTHEDOCS") == "True"


# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx_panels",
    "autoapi.extension",
]

autodoc_member_order = "bysource"

autodoc_mock_imports = [
    "pandas",
    "pytest",
    "numpy",
    "jax",
]

extlinks = {
    "ghuser": ("https://github.com/%s", "@"),
    "gh": ("https://github.com/OpenSourceEconomics/dags/pulls/%s", "#"),
}

intersphinx_mapping = {
    "numpy": ("https://numpy.org/doc/stable", None),
    "np": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "pd": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "python": ("https://docs.python.org/3.9", None),
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]
html_static_path = ["_static"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
source_suffix = ".rst"

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "dags"
copyright = f"2022, {author}"  # noqa: A001

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.

# The version, including alpha/beta/rc tags, but not commit hash and datestamps
release = version("dags")
# The short X.Y version.
version = ".".join(release.split(".")[:2])

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.

# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "**.ipynb_checkpoints"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

# If true, `todo` and `todoList` produce output, else they produce nothing.
if on_rtd:
    pass
else:
    todo_include_todos = True
    todo_emit_warnings = True

# Remove prefixed $ for bash, >>> for Python prompts, and In [1]: for IPython prompts.
copybutton_prompt_text = r"\$ |>>> |In \[\d\]: "
copybutton_prompt_is_regexp = True

# Configuration for autoapi
autoapi_type = "python"
autoapi_dirs = ["../../src"]
autoapi_keep_files = False
autoapi_add_toctree_entry = False


# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "pydata_sphinx_theme"

# html_logo = "_static/images/logo.svg"

html_theme_options = {
    "github_url": "https://github.com/OpenSourceEconomics/dags",
}

html_css_files = ["css/custom.css"]


# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]  # noqa: E800

# Custom sidebar templates, must be a dictionary that maps document names
# to template names.

# This is required for the alabaster theme
# refs: http://alabaster.readthedocs.io/en/latest/installation.html#sidebars
html_sidebars = {
    "**": [
        "relations.html",  # needs 'show_related': True theme option to display
        "searchbox.html",
    ]
}
