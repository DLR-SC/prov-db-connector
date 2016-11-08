from .provapi import ProvDbException


class ConverterException(ProvDbException):
    """
    Base exception class for document converter.
    """
    pass


class ParseException(ConverterException):
    """
    Thrown, if a given statement could not ne parsed.
    """
    pass


class NoDocumentException(ConverterException):
    """
    Thrown, if no document argument is passed.
    """
    pass


class SerializerException(ProvDbException):
    """
    Base exception class for serializer.
    """
    pass


class ValidatorException(ProvDbException):
    """
    Base exception class for validator.
    """
    pass
