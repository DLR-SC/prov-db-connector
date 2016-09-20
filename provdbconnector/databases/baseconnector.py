from abc import ABC, abstractmethod


class BaseConnector(ABC):

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect(self):
        pass
