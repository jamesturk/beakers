class DatabeakerError(Exception):
    """Base class for exceptions in this module."""


class InvalidGraph(DatabeakerError):
    """Raised when a graph is invalid."""


class SeedError(DatabeakerError):
    """Raised when a seed fails to run."""


class ItemNotFound(DatabeakerError):
    """Raised when an item is not found in a beaker."""
