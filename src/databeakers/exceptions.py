class BeakerNotFound(Exception):
    """Raised when a beaker is not found."""


class InvalidGraph(Exception):
    """Raised when a graph is invalid."""


class SeedError(Exception):
    """Raised when a seed fails to run."""


class ItemNotFound(Exception):
    """Raised when an item is not found in a beaker."""
