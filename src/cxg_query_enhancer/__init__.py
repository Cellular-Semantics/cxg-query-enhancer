from .enhancer import enhance, OntologyExtractor, SPARQLClient

# Note: _filter_ids_against_census is internal, so not typically re-exported here

# Get version from pyproject.toml using importlib.metadata (Python 3.8+)
try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("cxg-query-enhancer")  # Matches 'name' in pyproject.toml
    except PackageNotFoundError:
        # package is not installed, e.g., when running source directly
        __version__ = "unknown"
except ImportError:
    # Fallback for Python < 3.8 or if importlib.metadata is not available
    __version__ = "unknown"

__all__ = [
    "enhance",
    "OntologyExtractor",
    "SPARQLClient",
    "__version__",
]
