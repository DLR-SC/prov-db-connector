class ProvApiException(Exception):
    """
    Base exception class for all api exceptions.
    """
    pass


class NoDataBaseAdapterException(ProvApiException):
    """
    Thrown, if no database adapter argument is passed to the api class.
    """
    pass


class InvalidArgumentTypeException(ProvApiException):
    """
    Thrown, if an invalid argument is passed to any api method.
    """
    pass


class InvalidProvRecordException(ProvApiException):
    """"
    Thrown, if an invalid record is passed to any api method.
    """
    pass
