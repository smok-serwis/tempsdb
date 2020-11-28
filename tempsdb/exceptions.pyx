class TempsDBError(Exception):
    """Base class for TempsDB errors"""


class DoesNotExist(TempsDBError):
    """The required resource does not exist"""


class Corruption(TempsDBError):
    """Corruption was detected in the dataset"""
