# WaffleDB Error Classes


class WaffleDBError(Exception):
    """Base exception for WaffleDB."""
    pass


class ConnectionError(WaffleDBError):
    """Raised when connection to WaffleDB fails."""
    pass


class TimeoutError(WaffleDBError):
    """Raised when a request times out."""
    pass


class ValidationError(WaffleDBError):
    """Raised when input validation fails."""
    pass


class NotFoundError(WaffleDBError):
    """Raised when a resource is not found."""
    pass

