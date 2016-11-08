from .provapi import ProvDbException


class AdapterException(ProvDbException):
    """
    Base exception class for database adapters.
    """
    pass


class InvalidOptionsException(AdapterException):
    """
    Thrown, if passed argument for adapter is invalid.
    """
    pass


class AuthException(AdapterException):
    """
    Thrown, if database adapter could not establish a connection with given credentials to the database.
    """
    pass


class DatabaseException(AdapterException):
    """
    Thrown, if method could not performed on database.
    """
    pass


class CreateRecordException(DatabaseException):
    """
    Thrown, if record could not be saved in database.
    """
    pass


class CreateRelationException(DatabaseException):
    """
    Thrown, if relation could not be saved in database.
    """
    pass


class NotFoundException(DatabaseException):
    """
    Thrown, if record or relation could not be found in database.
    """
    pass


class MergeException(DatabaseException):
    """
    Thrown, if a record or relation can't get merged
    """
    pass
