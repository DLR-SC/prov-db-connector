class ProvApiException(Exception):
    pass


class NoDataBaseAdapterException(ProvApiException):
    pass


class InvalidArgumentTypeException(ProvApiException):
    pass


class InvalidProvRecordException(ProvApiException):
    pass
