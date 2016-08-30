from abc import ABCMeta, abstractmethod


class DocumentParser():
    __metaclass__ = ABCMeta

    @abstractmethod
    def parse(self):
        """
        parse given document to filter the data

        :return: the filtered data
        """
        pass