from abc import ABC, abstractmethod

class ConnectorException (Exception):
    pass

class InvalidOptionsException(ConnectorException):
    pass

class AuthException(ConnectorException):
    pass

class DatabaseException(ConnectorException):
    pass

class BaseConnector(ABC):

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect(self, authentication_info):
        pass
