from DocumentParser import DocumentParser
from bs4 import BeautifulSoup
import email


class MyHTMLParser(DocumentParser):
    def __init__(self, document):
        self.document = document
        self.content = ''

    def __get_content(self):
        """
        Get message content from MIME document

        """
        file = open(self.document)
        message = email.message_from_file(file)

        self.content = MyHTMLParser.__get_payload_string(message)


    @staticmethod
    def __get_payload_string(message):
        """
        Recursively retrieve payloads from message

        :return: The flattened text from message
        """

        if message.is_multipart():
            output = ''

            for m in message.get_payload():
                output += MyHTMLParser.__get_payload_string(m)

            return output
        else:
            payload = message.get_payload()

            if message.get_content_type() == 'text/html':
                soup = BeautifulSoup(payload, 'html.parser')
                payload = soup.get_text()
            else:
                payload = payload.decode('UTF8', 'ignore')

            return payload


    # def get_html_content(self):
    #     """
    #     set self.content as only the HTML content in HTML format from self.document
    #
    #     """
    #     with open(self.document, 'r') as doc_read:
    #         lines = doc_read.read().splitlines()
    #         read = False
    #         for line in lines:
    #             if line.lower().startswith('<html>'):
    #                 read = True
    #             if read:
    #                 if not line.__contains__('.jpg'):
    #                     self.content += line
    #             if line.lower().endswith('</html>'):
    #                 read = False

    def parse(self):
        """
        Parses HTML document to only get the useful data

        :return: the cleaned data from the HTML document
        """
        self.__get_content()
        self.soup = BeautifulSoup(self.content, 'html.parser')
        return self.soup.get_text()
