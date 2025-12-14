# WaffleDB Error Classes
# These errors provide helpful messages to guide users to solutions


class WaffleDBError(Exception):
    """Base exception for WaffleDB.
    
    When you see this, check the error message for:
    - What went wrong
    - Why it happened
    - How to fix it
    """
    pass


class ConnectionError(WaffleDBError):
    """Raised when connection to WaffleDB fails.
    
    This usually means:
    - WaffleDB server is not running
    - Server is not at the URL you specified
    - There's a network issue
    
    Fix:
    - Start server: docker run -p 8080:8080 waffledb
    - Check server URL is correct
    - Check network connectivity
    """
    pass


class TimeoutError(WaffleDBError):
    """Raised when a request times out.
    
    This usually means:
    - WaffleDB server is taking too long to respond
    - Your request is too large
    - Server is under heavy load
    
    Fix:
    - Increase timeout in client config
    - Try smaller batch sizes
    - Check server health/logs
    """
    pass


class ValidationError(WaffleDBError):
    """Raised when input validation fails.
    
    This usually means:
    - You passed invalid data (wrong type, format, etc.)
    - Missing required fields
    - Data constraints violated
    
    Fix:
    - Check the error message for what field is invalid
    - Verify data types (embeddings must be lists of floats)
    - Check required fields are present
    """
    pass


class NotFoundError(WaffleDBError):
    """Raised when a resource is not found.
    
    This usually means:
    - Collection doesn't exist
    - Vector ID doesn't exist
    - Typo in name
    
    Fix:
    - Use db.list() to see available collections
    - Create collection first: db.add("name", ...)
    - Check spelling of collection/vector name
    """
    pass


