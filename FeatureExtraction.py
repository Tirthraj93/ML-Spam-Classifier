import random, sys

from Constants import *
from FileUtil import read_word_into_dict, list_to_file, append_list_to_file
from ElasticSearchUtility import ElasticSearchUtility


# FEATURE EXTRACTION
# get list of ngrams as features
# fetch feature values for each ngram from ElasticSearch
# generate feature matrix

WRITE_BUFFER_SIZE = 100


def extract_features(feature_type, suffix):
    """
    extract features based on feature type from ElasticSearch and store it in a feature matrix file

    :param feature_type: type of features (either "manual", "shingle" or "skipgrams"
    :param suffix: suffix for file names
    """

    if feature_type == "manual":
        # get train and test ids
        train_ids, test_ids = get_ids()
        # store train and test ids
        list_to_file(train_ids, TRAIN_ID_FILE + suffix)
        list_to_file(test_ids, TEST_ID_FILE + suffix)
        # extract features for ids
        extract_manual_features("body_shingles", train_ids, test_ids, suffix)
    elif feature_type == "shingle":
        # get train and test ids
        train_ids, test_ids = get_ids()
        # store train and test ids
        list_to_file(train_ids, TRAIN_ID_FILE + suffix)
        list_to_file(test_ids, TEST_ID_FILE + suffix)
        # extract features for ids
        extract_gram_features("body_shingles", train_ids, test_ids, suffix)
    elif feature_type == "skipgram":
        # get train and test ids
        train_ids, test_ids = get_ids()
        # store train and test ids
        list_to_file(train_ids, TRAIN_ID_FILE + suffix)
        list_to_file(test_ids, TEST_ID_FILE + suffix)
        # extract features for ids
        extract_gram_features("body_skipgrams", train_ids, test_ids, suffix)
    else:
        print 'Invalid feature_type for function extract_features.'
        return None


def get_ids():
    """
    Get all ids from ElasticSearch and get 80% of them as train ids and 20% of them as test ids

    :return: train ids and test ids respectively
    """

    # train_id_query = ElasticSearchUtility.get_match_query("split", "train")
    # test_id_query = ElasticSearchUtility.get_match_query("split", "test")

    match_all_query = {
        "query": {
            "match_all": {}
        }
    }

    es_util = ElasticSearchUtility()
    all_ids = es_util.get_all_ids(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, match_all_query)

    # split all document ids into two lists - 80% of them and 20% of them
    random.shuffle(all_ids)
    train_size = all_ids.__len__() * TRAIN_PERCENT / 100
    all_train_ids = all_ids[:train_size]
    all_test_ids = all_ids[train_size:]

    return all_train_ids, all_test_ids


def extract_manual_features(field, train_ids, test_ids, gram_type):
    """
    Read features file to fetch manual features, extract corresponding values from ElasticSearch
    and store it in feature_matrix

    :param field: name of the field containing feature grams
    :param train_ids: ids of documents to train
    :param test_ids: ids of documents to test
    :param gram_type: type of gram (_unigram, _skipgram, etc)
    """

    features_dict = read_word_into_dict(MANUAL_SPAM_FEAT_FILE)

    create_feature_matrix(features_dict, field, train_ids, FM_TRAIN_FILE + gram_type, manual_terms=True)
    create_feature_matrix(features_dict, field, test_ids, FM_TEST_FILE + gram_type, manual_terms=True)


def extract_gram_features(field, train_ids, test_ids, gram_type, unigram=False):
    """
    Read features file to fetch terms from ElasticSearch, extract corresponding values from ElasticSearch
    and store it in feature_matrix

    :param field: name of the field containing feature grams
    :param train_ids: ids of documents to train
    :param test_ids: ids of documents to test
    :param gram_type: type of gram (_unigram, _skipgram, etc)
    :param unigram: True if term to fetch has to be only unigram
    """

    if not os.path.isfile(DATA_PATH + gram_type):
        es_util = ElasticSearchUtility()
        grams = es_util.get_all_grams(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, field, unigram)
        list_to_file(grams, DATA_PATH + gram_type)

    all_grams_dict = read_word_into_dict(DATA_PATH + gram_type)

    create_feature_matrix(all_grams_dict, field, train_ids, FM_TRAIN_FILE + gram_type)
    create_feature_matrix(all_grams_dict, field, test_ids, FM_TEST_FILE + gram_type)


def create_feature_matrix(all_features_dict, field, ids_list, feature_matrix_file, manual_terms=False):
    """
    Retrieve feature matrix for given features list and field using ES termvectors
    in format {id: [{feature:value}, {feature:value}]}

    :param all_features_dict: dictionary of format {feature:index}
    :param field: name of the field containing feature grams
    :param ids_list: ids of the documents for which geatures are to be extracted
    :param feature_matrix_file: file to append feature matrix to
    :return: dictinary of format {id: [{feature:value}, {feature:value}]}
    """

    # [[{feature1:value}, {feature2:value}], [{feature1:value}, {feature2:value}], ...]
    feature_matrix = []
    doc_id_list = []
    write_list = []
    es_util = ElasticSearchUtility()

    doc_label_dict = es_util.get_field_values(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE, "label")

    doc_count = 0
    print_count = 1

    for doc_id in ids_list:

        print print_count, ' - docs processed'

        if manual_terms:
            features_dict = es_util.get_sparse_tf_features(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE,
                                                            field, doc_id, terms_to_include=all_features_dict.keys())
        else:
            features_dict = es_util.get_sparse_tf_features(TREC_SPAM_CORPUS_INDEX, TREC_SPAM_CORPUS_TYPE,
                                                            field, doc_id)

        feature_matrix.append(features_dict)
        doc_id_list.append(doc_id)

        doc_count += 1
        print_count += 1

        # begins the logic for writing feature matrix into file

        # add label
        label = doc_label_dict[doc_id]
        output_string = label + ' '
        write_features = dict()

        # add feature values
        for feature in features_dict:
            try:
                feature_index = all_features_dict[feature]
                feature_tf = features_dict[feature]
                write_features[feature_index] = str(feature_index) + ":" + str(feature_tf) + ' '
            except KeyError:
                pass

        # sort features in ascending order
        write_features = sorted(write_features.items(), key=lambda x: x[0])

        # append doc line to write list
        write_list.append(output_string + ''.join(x[1] for x in write_features))

        # write
        if doc_count == WRITE_BUFFER_SIZE:
            append_list_to_file(write_list, feature_matrix_file)
            write_list = []
            doc_count = 0

    # write
    append_list_to_file(write_list, feature_matrix_file)


def get_feature_matrix(features_list, field):
    """
    Creates feature matrix for given features list and field of format
    {id: [{feature:value}, {feature:value}]}

    :param features_list: list of features
    :param field: name of the field containing feature grams
    :return: dictinary of format {id: [{feature:value}, {feature:value}]}
    """

    # {id: [{feature:value}, {feature:value}]}
    feature_matrix = dict()

    es_util = ElasticSearchUtility()
    feature_index = 1

    for features in features_list:

        feature_list = features.split(' ')

        for feature in feature_list:

            feature = feature.lower()
            print 'extracting - ', feature

            # {id: tf}
            curr_feature_matrix = es_util.single_feature_matrix(TREC_SPAM_CORPUS_INDEX,
                                                                TREC_SPAM_CORPUS_TYPE, field, feature)
            for doc_id in curr_feature_matrix:
                tf = curr_feature_matrix[doc_id]
                if doc_id in feature_matrix:
                    feature_matrix[doc_id].append({feature_index:tf})
                else:
                    feature_matrix[doc_id] = [{feature_index:tf}]

            feature_index += 1

    return feature_matrix


def store_feature_matrix(doc_id_list, docs_features, features_list, feature_matrix_file, doc_label_dict):
    """
    For given id list generate feature matrix using feature matrix json and features list,
    and store it in given feature matrix file in the format <label> <feature-1>:<value-1> ... <feature-n>:<value-n>

    :param doc_id_list: list of doc ids to store features of
    :param docs_features: array of features of a doc [[feature1:value1, feature2:value2, ...], [feature1:value1], ...]
    :param features_list: list of all features
    :param id_list: list of all ids
    :param feature_matrix_file: file to store feature matrix in
    :param doc_label_dict: dictionary containing label for all documents
    """

    print 'storing...'

    output_list = []

    index = 0

    for features_dict in docs_features:

        doc_id = doc_id_list[index]
        label = doc_label_dict[doc_id]
        index += 1

        output_string = label + ' '

        feature_index = 1

        for feature in features_list:
            try:
                feature_tf = features_dict[feature]
                output_string += str(feature_index) + ":" + str(feature_tf) + ' '
            except KeyError:
                pass
            feature_index += 1

        output_list.append(output_string)

    append_list_to_file(output_list, feature_matrix_file)
