from abc import ABC, abstractmethod

class ConnectorException (Exception):
    pass

class InvalidOptionsException(ConnectorException):
    pass

class AuthException(ConnectorException):
    pass

class DatabaseException(ConnectorException):
    pass

class CreateNodeException(DatabaseException):
    pass


class BaseConnector(ABC):

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect(self, authentication_info):
        pass

    @abstractmethod
    def create_document(self):
        pass

    @abstractmethod
    def create_bundle(self, document_id,attributes, metadata):
        pass


    @abstractmethod
    def create_record(self, bundle_id, attributes, metadata):
        pass
