from .provapi import ProvApiException


class ConverterException(ProvApiException):
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


class SerializerException(ProvApiException):
    """
    Base exception class for serializer.
    """
    pass


class ValidatorException(ProvApiException):
    """
    Base exception class for validator.
    """
    pass
