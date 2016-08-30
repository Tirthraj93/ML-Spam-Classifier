import random
from os import listdir

from Constants import TRAIN_PERCENT, EMAIL_DATA_PATH, LABELS_PATH, \
    TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, TREC_SPAM_CORPUS_BODY
from MyEmailParser import MyHTMLParser
from ElasticSearchUtility import ElasticSearchUtility
from FileUtil import read_labels


# DATA INSERTION
# clean the email data
# get manual classification
# store in ElasticSearch (80% train, 20% test)

EMAIL_FILES = listdir(EMAIL_DATA_PATH)

def insert_data():
    """
    Clean and store all documents into ElasticSearch
    """

    # read document labels into dictionary
    doc_labels_dict = read_labels(LABELS_PATH)

    # split all documents into two lists - 80% of them and 20% of them
    random.shuffle(EMAIL_FILES)
    train_size = EMAIL_FILES.__len__() * TRAIN_PERCENT / 100
    train_docs = EMAIL_FILES[:train_size]
    test_docs = EMAIL_FILES[train_size:]

    # clean documents
    train_store_docs = __get_store_docs(train_docs, doc_labels_dict, 'train')
    test_store_docs = __get_store_docs(test_docs, doc_labels_dict, 'test')

    # store documents into ElasticSearch
    es_util = ElasticSearchUtility()
    es_util.create_index(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_BODY)
    es_util.store_index(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, train_store_docs, 1)
    es_util.store_index(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, test_store_docs, train_size + 1)


def __get_store_docs(docs_list, doc_labels_dict, split_string):
    """
    Cleans all document in given list and returns list of dictionary containing fields
    to store in ElasticSearch for each doc

    :param docs_list: docs to clean
    :param doc_labels_dict: dictionary containing labels for each document
    :param split_string: split string for documents to store (train or test)
    :return: list of dictionary containing fields to store in ElasticSearch for each doc
    """
    clean_docs = []

    docs_cleaned = 0

    for doc in docs_list:
        parser = MyHTMLParser(EMAIL_DATA_PATH + doc)
        clean_text = parser.parse()
        source_dict = {
            "_source": {
                "file_name": doc,
                "label": doc_labels_dict[doc],
                "split": split_string,
                "body_shingles": clean_text,
                "body_skipgrams": clean_text
            }
        }
        clean_docs.append(source_dict)

        print split_string + ' - ' + str(docs_cleaned)
        docs_cleaned += 1

    return clean_docs
