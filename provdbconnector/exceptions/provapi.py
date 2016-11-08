class ProvDbException(Exception):
    """
    Base exception class for all api exceptions.
    """
    pass


class NoDataBaseAdapterException(ProvDbException):
    """
    Thrown, if no database adapter argument is passed to the api class.
    """
    pass


class InvalidArgumentTypeException(ProvDbException):
    """
    Thrown, if an invalid argument is passed to any api method.
    """
    pass


class InvalidProvRecordException(ProvDbException):
    """"
    Thrown, if an invalid record is passed to any api method.
    """
    pass
