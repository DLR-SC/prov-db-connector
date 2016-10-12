from .provapi import ProvApiException


class AdapterException(ProvApiException):
    pass


class InvalidOptionsException(AdapterException):
    pass


class AuthException(AdapterException):
    pass


class DatabaseException(AdapterException):
    pass


class CreateRecordException(DatabaseException):
    pass


class CreateRelationException(DatabaseException):
    pass


class NotFoundException(DatabaseException):
    pass
