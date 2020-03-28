# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import sphinx_material

sys.path.insert(0, os.path.abspath('../../'))

# -- Project information -----------------------------------------------------

project = 'PyStream'
copyright = '2020, Yahor Paromau'
author = 'Yahor Paromau'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx_material'
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Register the theme as an extension to generate a sitemap.xml

# Choose the material theme
html_theme = 'sphinx_material'
# Get the them path
html_theme_path = sphinx_material.html_theme_path()
# Register the required helpers for the html context
html_context = sphinx_material.get_html_context()

html_theme_options = {
    'color_primary': 'blue',
    'color_accent': 'light-blue',
    'repo_name': 'PyStream-API',
    'repo_url': 'https://github.com/RikiTikkiTavi/PyStream-API',
    'nav_title': 'PyStream',
    'logo_icon': '&#xe869',
    'heroes': {
        'index': 'Functional style lazy array processing methods. Java StreamApi analogue.',
    },
    'master_doc': False,
    'nav_links': [],
    'globaltoc_depth': 3,
    'globaltoc_collapse': True,
    'globaltoc_includehidden': True,
}

# Custom sidebar templates, maps document names to template names.
html_sidebars = {
    "**": ["logo-text.html", "globaltoc.html", "localtoc.html", "searchbox.html"]
}