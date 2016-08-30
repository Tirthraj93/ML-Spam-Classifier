from FileUtil import read_word_into_list, read_predict_result_into_list, list_to_file
from ElasticSearchUtility import ElasticSearchUtility
from Constants import TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE


class ResultsProcessing():

    def __init__(self, id_file, result_file):
        self.id_list = read_word_into_list(id_file) # [doc1_id, doc2_id, ...]
        self.result_list = read_predict_result_into_list(result_file) # [res1, res2, ...]
        self.get_all_results()

    def get_all_results(self):
        """
        Get all results in a dictionary {id:result}

        """

        print 'Getting all results...'

        self.all_results_dict = dict()

        id_length = len(self.id_list)

        for i in range(0, id_length):
            doc_id = self.id_list[i]
            result = self.result_list[i]
            self.all_results_dict[doc_id] = result

    def store_top_n_results(self, n, out_file):
        """
        Store top n results for given n into given out_file from self.all_results
        where results are fetched file names from ElasticSearch using their doc ids

        :param n: number of top results to store
        :param out_file: output file for storing results
        """

        print 'Getting top ', n, ' results...'

        top_n = sorted(self.all_results_dict.items(), key=lambda x: x[1], reverse=False)[:n]

        es_util = ElasticSearchUtility()

        top_n_ids = [x[0] for x in top_n]

        top_n_files = es_util.get_field_values_for_docs(TREC_SPAM_CORPUS_INDEX,
                                                        TREC_SPAM_CORPUS_TYPE,
                                                        "file_name", top_n_ids)

        list_to_file(top_n_files, out_file)
