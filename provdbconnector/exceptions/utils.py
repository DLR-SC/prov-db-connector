from .provapi import ProvApiException

class ConverterException(ProvApiException):
    pass

class ParseException(ConverterException):
    pass

class NoDocumentException(ConverterException):
    pass

class SerializerException(ProvApiException):
    pass

class ValidatorException(ProvApiException):
    pass
