from elasticsearch import Elasticsearch
from elasticsearch import helpers

from Constants import ES_HOST


class ElasticSearchUtility:
    """
    class to communicate with ElasticSearch
    """

    def __init__(self):
        self.es = Elasticsearch(hosts=[ES_HOST], timeout=750)

    def index_exists(self, index_name):
        return self.es.indices.exists(index_name)

    def create_index(self, index_name, body):
        """
        Created a new index. If it already exists, deletes that first.

        :param index_name: index to create
        :param body: index creation body
        """
        if self.es.indices.exists(index_name):
            print("deleting '%s' index..." % index_name)
            res = self.es.indices.delete(index=index_name)
            print(" response: '%s'" % res)

        print("creating '%s' index..." % index_name)
        res = self.es.indices.create(index=index_name, body=body)
        print(" response: '%s'" % res)

    def get_doc_count(self, index_name, doc_type):
        """
        Get total number of documents in a given index

        :param index_name: name of the index
        :param doc_type: type of the document
        :return: total number of documents
        """
        return self.es.count(index_name, doc_type)["count"]

    def store_index(self, index, doc_type, source_list, init_id):
        """
        Store all data in source list as a unique document in given ElasticSearch index-type

        :param index: name of the index
        :param doc_type: type of the document
        :param source_list: list of document source to insert into given index-type
        :param init_id: initial id for the document
        """

        bulk_actions = []
        doc_id = init_id

        for source in source_list:
            data_body = ElasticSearchUtility.__index_data_body(index, doc_type, doc_id, source["_source"])
            bulk_actions.append(data_body)
            doc_id += 1

        print 'inserting - ', len(bulk_actions)
        helpers.bulk(self.es, bulk_actions)

    def get_all_terms(self, index, doc_type, doc_id, field):
        """
        Get all terms for given field of given index-doc_type-doc_id

        :param index: name of the index
        :param doc_type: type of the document
        :param doc_id: id of the document
        :param field: field to get term vectors of
        :return: all terms for given document
        """

        term_vector = self.es.termvectors(index, doc_type, id=doc_id, field_statistics=False,
                                          fields=[field], offsets=False, positions=False)

        all_terms = term_vector[field]["terms"].keys()

        return all_terms

    def get_all_ids(self, index_name, doc_type, query_body):
        """
        Returns all ids of given index for given query

        :param index_name: Name of the index
        :param doc_type: Type of the document
        :param query_body: search query
        :return: List of ids of entire index
        """

        print 'getting all ids...'

        # query scroll
        id_list = []

        scroll = self.es.search(
            index=index_name,
            doc_type=doc_type,
            scroll='10m',
            size=10000,
            fields=['_id'],
            body=query_body)

        scroll_size = scroll['hits']['total']
        size = 0
        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']
            for hit in hits_list:
                doc_id = hit['_id']
                id_list.append(doc_id)
            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled - ", str(size)
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
        return id_list

    def single_feature_matrix(self, index, doc_type, field, feature):
        """
        Fetch all documents containing given feature along with its tf as a score from
        ElasticSearch in format {id: tf}

        :param index: name of the index
        :param doc_type: type of the document
        :param field: the field to extract features from
        :param feature: the feature to extract
        :return: the dictionary of the format {id: tf}
        """

        out_dict = dict()

        query_body = {
            "query": {
                "function_score": {
                    "query": {
                        "term": {
                            "body_shingles": {
                                "value": feature
                            }
                        }
                    },
                    "functions": [
                        {
                            "script_score": {
                                "script": {
                                    "file": "getFeatureValue",
                                    "params": {
                                        "term": feature,
                                        "field": field
                                    }
                                }
                            }
                        }
                    ],
                    "boost_mode": "replace"
                }
            }
        }

        # query scroll
        scroll = self.es.search(
            index=index,
            doc_type=doc_type,
            scroll='10m',
            size=10000,
            body=query_body,
            fields=["stream_id"])

        # set initial scroll size
        scroll_size = scroll['hits']['total']

        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']

            for hit in hits_list:
                out_dict[hit["_id"]] = hit["_score"]

            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')

        return out_dict

    @staticmethod
    def __index_data_body(index, doc_type, doc_id, source):
        """
        Create index data body for insertion based on given parameters

        :param index: name of the index
        :param doc_type: type of the document
        :param doc_id: unique id for index source
        :param source: data source
        :return: index data to insert
        """

        index_data = {
            "_index": index,
            "_type": doc_type,
            "_id": doc_id,
            "_source": source
        }

        return index_data

    @staticmethod
    def get_match_query(field, value):
        """
        creates match query body for given field and value

        :param field: document field
        :param value: value for the field
        :return: the query body
        """
        query_body = {
            "query": {
                "match": {
                    field: value
                }
            }
        }

        return query_body

    def get_field_values(self, index, doc_type, field):
        """
        Get dictionary of id:field_value for given index-type and field

        :param index: name of the index
        :param doc_type: type of the document
        :param field: field to get value of
        :return: id:field_value dictionary
        """

        out_dict = dict()

        query_body = {
            "query": {
                "match_all": {}
            }
        }

        # query scroll
        scroll = self.es.search(
            index=index,
            doc_type=doc_type,
            scroll='10m',
            size=10000,
            body=query_body,
            fields=[field])

        # set initial scroll size
        scroll_size = scroll['hits']['total']

        # retrieve results
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']

            for hit in hits_list:
                doc_id = hit["_id"]
                field_value = hit["fields"][field][0]
                out_dict[doc_id] = field_value

            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')

        return out_dict

    def get_all_grams(self, index, doc_type, field, unigrams=False):
        """
        Get all unique grams from entire index for given field if unigrams is False,
        otherwise get only unigrams

        :param index: name of the index
        :param doc_type: type of the document
        :param field: name of the index field
        :return: the set of all grams
        """

        print 'Getting all grams...'

        grams = set()

        if unigrams:
            file = "getUnigrams"
        else:
            file = "getGrams"

        query_body = {
            "script_fields": {
                "grams": {
                    "script": {
                        "file": file,
                        "params": {
                            "field": field
                        }
                    }
                }
            }
        }

        # query scroll
        scroll = self.es.search(
            index=index,
            doc_type=doc_type,
            scroll='10m',
            size=1000,
            body=query_body)

        # set initial scroll size
        scroll_size = scroll['hits']['total']

        # retrieve results
        size = 0
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']

            for hit in hits_list:
                try:
                    field_value = hit["fields"]["grams"]
                    grams.update(set([value.encode('UTF8') for value in field_value]))
                except KeyError:
                    pass

            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')

            size += scroll_size
            print size

        return grams

    def get_sparse_tf_features(self, index, doc_type, field, doc_id, terms_to_include=None):
        """
        Get an dictionary of format {term1:tf1, term2:tf2, ...} for all terms in given field of given index's doc_id
        where tf id greater than 0

        :param index: name of the index
        :param doc_type: type of the document
        :param field: field to get terms of
        :param doc_id: index document id
        :return: dictionary of the format {term1:tf1, term2:tf2, ...} for all terms having tf > 0
        """

        out_dict = dict()

        # POST trec_spam/documents/1/_termvector?field_statistics=false
        # &positions=false&offsets=false
        # &fields=body_shingles
        response = self.es.termvectors(index, doc_type, doc_id, fields=[field],
                                       field_statistics=False, positions=False, offsets=False)

        try:
            terms = response["term_vectors"][field]["terms"]
        except KeyError:
            return dict()

        for term in terms:
            words_in_term = len(term.split(' '))
            tf = terms[term]["term_freq"]

            if tf > 0 and words_in_term == 1:
                try:
                    decoded_term = str(term)

                    if terms_to_include is None:
                        out_dict[decoded_term] = tf
                    else:
                        if terms_to_include.__contains__(decoded_term):
                            out_dict[decoded_term] = tf
                except:
                    pass

        return out_dict

    def get_field_values_for_docs(self, index, doc_type, field, docs_list):
        """
        Get value of given field for all given docs in given order

        :param index: name of the index
        :param doc_type: type of the document
        :param field: field to retirieve value of
        :param docs_list: list of documents for whic values are to be retrieved
        :return: list of values of given field for their corresponding ddcs
        """

        values_list = []

        for doc in docs_list:
            response = self.es.get(index, doc, doc_type=doc_type, fields=[field])
            value = str(response["fields"][field][0])
            values_list.append(value)

        return values_list
