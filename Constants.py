import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_PATH = BASE_DIR + "\\Data\\"
TRAIN_PATH = DATA_PATH + "train\\"
TEST_PATH = DATA_PATH + "test\\"

FM_MANUAL_JSON_FILE = DATA_PATH + "fm_manual_json"
FM_TRAIN_FILE = TRAIN_PATH + "feature_matrix_train"
MODEL_FILE = FM_TRAIN_FILE + '.model'
FM_TEST_FILE = TEST_PATH + "feature_matrix_test"
TRAIN_ID_FILE = TRAIN_PATH + "train_ids"
TEST_ID_FILE = TEST_PATH + "test_ids"
OUTPUT_FILE = DATA_PATH + "output"
TOP_RESULTS_FILE = DATA_PATH + "top_n"
# Storage Details
ES_HOST = dict(host="localhost", port=9200)

# path to 2007 TREC Public SPAM Corpus
TREC_CORPUS_PATH = DATA_PATH + "trec07p\\"
# directory containing emails to classify
EMAIL_DATA_PATH = TREC_CORPUS_PATH + 'data\\'
# classification index path
LABELS_PATH = TREC_CORPUS_PATH + 'full\\index'
# manual spam grams
MANUAL_SPAM_FEAT_FILE = TREC_CORPUS_PATH + 'spam_words.txt'

TRAIN_PERCENT = 85

TREC_SPAM_CORPUS_INDEX = "trec_spam"
TREC_SPAM_CORPUS_TYPE = "documents"
TREC_SPAM_CORPUS_BODY = {
    "settings": {
        "index": {
            "store": {
                "type": "default"
            },
            "number_of_shards": 3,
            "number_of_replicas": 0
        },
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "my_shingle_filter": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 3,
                    "output_unigrams": True
                },
                "ngram_filter": {
                    "type": "nGram",
                    "min_gram": 2,
                    "max_gram": 4
                }
            },
            "analyzer": {
                "my_shingle_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "my_shingle_filter"
                    ]
                },
                "ngram_filter_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "ngram_filter"
                    ]
                }
            }
        }
    },
    "mappings": {
        TREC_SPAM_CORPUS_TYPE: {
            "properties": {
                "file_name": {
                    "type": "string",
                    "store": "true",
                },
                "label": {
                    "type": "string",
                    "store": "true",
                },
                "split": {
                    "type": "string",
                    "store": "true",
                },
                "body_shingles": {
                    "type": "string",
                    "store": "true",
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "my_shingle_analyzer"
                },
                "body_skipgrams": {
                    "type": "string",
                    "store": "true",
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "ngram_filter_analyzer"
                }
            }
        }
    }
}
