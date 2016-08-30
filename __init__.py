# from liblinearutil import *
import time

from DataInsertion import insert_data
from FeatureExtraction import extract_features
from ResultsProcessing import ResultsProcessing
from Constants import TEST_ID_FILE, OUTPUT_FILE, TOP_RESULTS_FILE


FEATURE_TYPE = "manual"
SUFFIX = "_manual"
N = 50


if __name__ == '__main__':
    # DATA INSERTION
    # clean the email data
    # get manual classification
    # store in ElasticSearch (80% train, 20% test)
    start_time = time.time()
    insert_data()
    end_time = time.time()
    print 'Execution Time: ', (end_time - start_time) / 60

    # FEATURE EXTRACTION
    # get list of ngrams as features
    # fetch feature values for each ngram from ElasticSearch
    # generate feature matrix
    start_time = time.time()
    extract_features(FEATURE_TYPE, SUFFIX)
    end_time = time.time()
    print 'Execution Time: ', (end_time - start_time) / 60

    # TRAINING AND TESTING
    # train
    # test
    # train ML model on training matrix
    # y, x = svm_read_problem(MANUAL_FM_TRAIN_FILE)

    # -s type : set type of solver (default 1)
    #   for multi-class classification
    # 	 0 -- L2-regularized logistic regression (primal)
    # 	 1 -- L2-regularized L2-loss support vector classification (dual)
    # 	 2 -- L2-regularized L2-loss support vector classification (primal)
    # 	 3 -- L2-regularized L1-loss support vector classification (dual)
    # 	 4 -- support vector classification by Crammer and Singer
    # 	 5 -- L1-regularized L2-loss support vector classification
    # 	 6 -- L1-regularized logistic regression
    # 	 7 -- L2-regularized logistic regression (dual)
    #   for regression
    # 	11 -- L2-regularized L2-loss support vector regression (primal)
    # 	12 -- L2-regularized L2-loss support vector regression (dual)
    # 	13 -- L2-regularized L1-loss support vector regression (dual)
    # model = train(y, x, '-s 0')
    # save_model(MODEL_FILE, model)

    # CREATE RESULTS
    # rank documents according to test scores
    res_processing = ResultsProcessing(TEST_ID_FILE + SUFFIX, OUTPUT_FILE + SUFFIX)
    res_processing.store_top_n_results(N, TOP_RESULTS_FILE + SUFFIX)
