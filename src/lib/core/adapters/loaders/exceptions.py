"""Loader-level exceptions.

All loader adapter implementations should raise subclasses of :class:`LoaderError`
so callers can catch a single base type without importing adapter-specific classes.
"""

__all__ = ["LoaderError"]


class LoaderError(Exception):
    """Base exception for all document loader failures.

    Raised when a loader adapter determines that the input document cannot
    be processed — e.g. unsupported format, corrupt file, or a rejection
    returned by the underlying service.  Network/connectivity failures are
    NOT represented here; those surface as ``httpx.HTTPError`` and are
    mapped to ``UpstreamServiceException`` by the service layer.
    """
