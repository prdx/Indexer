from models.tf_idf_model import TFIDFModel
from models.okapi_bm25 import OkapiBM25
from models.laplace_unigram_lm_model import LaplaceUnigramLMModel
from models.proximity_search_model import ProximitySearchModel
from utils.constants import Constants
from utils.text import build_query_list, build_query_list_for_prox_search, get_stopwords, get_file_list, write_output
import sys
import os
import threading
import numpy as np
import pickle
import pprint
from collections import ChainMap

document_statistics = {}
tf_for_queries = {}
wfd_collection = {}
total_tf_wd = {}
query_list = {}
query_list_for_ps = {}
total_length = 0
term_maps_collection = {}

tf_idf_result = {}
laplace_result = {}
bm25_result = {}
ps_result = {}

def run_tf_idf():
    print("Processing: TF-IDF model")
    tfidf = TFIDFModel(document_statistics)
    for q_no in query_list:
        query = query_list[q_no]
        results = tfidf.query(query, wfd_collection, tf_for_queries[q_no])
        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v1: (k_v1[1],k_v1[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if value != 0:
                if q_no not in tf_idf_result:
                    tf_idf_result[q_no] = {
                            key: value
                            }
                else: tf_idf_result[q_no][key] = value
                write_output(
                        model = 'tfidf',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1
    print("TF-IDF Done")

def run_bm25():
    print("Processing: Okapi BM25 model")
    bm25 = OkapiBM25(document_statistics)
    for q_no in query_list:
        query = query_list[q_no]
        results = bm25.query(query, wfd_collection, tf_for_queries[q_no])
        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v2: (k_v2[1],k_v2[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT or value <= 0:
                break
            if value != 0:
                if q_no not in bm25_result:
                    bm25_result[q_no] = {
                            key: value
                            }
                else: bm25_result[q_no][key] = value
                write_output(
                        model = 'bm25',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1
    print("BM25 Done")

def run_proximity_search():
    print("Processing: Proximity Search")
    ps = ProximitySearchModel(document_statistics)
    for q_no in query_list_for_ps:
        query = query_list_for_ps[q_no]
        results = ps.query(query, term_maps_collection, wfd_collection, tf_for_queries[q_no])
        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v2: (k_v2[1],k_v2[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if q_no not in ps_result:
                ps_result[q_no] = {
                        key: value
                        }
            else: ps_result[q_no][key] = value
            write_output(
                    model = 'ps',
                    query_no = str(q_no),
                    doc_no = str(key),
                    rank = str(rank),
                    score = str(value))
            rank += 1
    print("Proximity Search done")

def run_laplace_unigram():
    print("Processing: Unigram LM with Laplace model")
    laplace_unigram = LaplaceUnigramLMModel(document_statistics)
    for q_no in query_list:
        query = query_list[q_no]
        results = laplace_unigram.query(query, tf_for_queries[q_no])
        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v3: (k_v3[1],k_v3[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if value != 0:
                if q_no not in laplace_result:
                    laplace_result[q_no] = {
                            key: value
                            }
                else: laplace_result[q_no][key] = value
                write_output(
                        model = 'laplace_unigram',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1
    print("Unigram LM with Laplace done")

def run_laplace_unigram_with_ps():
    print("Processing: Unigram LM with Laplace model with ps")
    for q_no in query_list:
        results = {}
        for doc in laplace_result[q_no]:
            if doc in ps_result:
                print("Here")
                results[doc] = laplace_result[q_no][doc] * ps_result[q_no][doc]
            else:
                results[doc] = laplace_result[q_no][doc]

        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v3: (k_v3[1],k_v3[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if value != 0:
                write_output(
                        model = 'laplace_unigram_ps',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1
                
def run_tf_idf_with_ps():
    print("Processing: TF-IDF model with ps")
    for q_no in query_list:
        results = {}
        for doc in tf_idf_result[q_no]:
            if doc in ps_result:
                results[doc] = tf_idf_result[q_no][doc] + ps_result[q_no][doc]
            else:
                results[doc] = tf_idf_result[q_no][doc]

        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v3: (k_v3[1],k_v3[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if value != 0:
                write_output(
                        model = 'tfidf_ps',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1

def run_bm25_with_ps():
    print("Processing: BM25 model with ps")
    for q_no in query_list:
        results = {}
        for doc in bm25_result[q_no]:
            if doc in ps_result:
                results[doc] = bm25_result[q_no][doc] + ps_result[q_no][doc]
            else:
                results[doc] = bm25_result[q_no][doc]

        rank = 1
        for key, value in sorted(iter(results.items()), key=lambda k_v3: (k_v3[1],k_v3[0]), reverse=True):
            if rank > Constants.MAX_OUTPUT:
                break
            if value != 0:
                write_output(
                        model = 'bm25_ps',
                        query_no = str(q_no),
                        doc_no = str(key),
                        rank = str(rank),
                        score = str(value))
                rank += 1
def build_document_statistics():
    print("Building document statistics")

    with open("./index/document_length.txt", "r") as stats:
        data = stats.read()
    data = data.split("\n")
    sum_ttf = 0
    for d in data:
        doc_stats = d.split(",")
        try:
            document_statistics[doc_stats[0]] = int(doc_stats[1])
            sum_ttf += int(doc_stats[1])
        except IndexError:
            break
    return sum_ttf

def clean_results_folder():
    print("Removing all files in the results folder")
    result_files = os.listdir(Constants.RESULTS_PATH)
    for result_file in result_files:
        os.remove(Constants.RESULTS_PATH + result_file)

def build_tf_for_queries():
    with open("./index/index.p.meta", "rb") as c:
        catalog = pickle.load(c)
    print("Collecting the tf values")
    for q_no in query_list:
        query = query_list[q_no]
        tf_collection  = []
        words = query.split(' ')
        for word in words:
            with open("./index/index.p", "rb") as i:
                if word in catalog:
                    i.seek(catalog[word][0])
                    data = pickle.load(i)
                    temp_tf = {}
                    temp_pos = {}
                    for d in data[1]:
                        # d[0]: document id, d[1]: ttf
                        temp_tf[d[0]] = d[1]
                        temp_pos[d[0]] = d[2]
                    w_d = len(temp_tf)
            # w_d, tf = get_term_statistics(word)
            wfd_collection[word] = w_d
            term_maps_collection[word] = temp_pos
            tf_collection.append(temp_tf)
        tf_for_queries[q_no] = tf_collection

def build_term_maps_for_queries():
    with open("./index/index.p.meta", "rb") as c:
        catalog = pickle.load(c)
    print("Collecting the tf values")
    for q_no in query_list_for_ps:
        query = query_list_for_ps[q_no]
        tf_collection  = []
        words = query.split(' ')
        for word in words:
            with open("./index/index.p", "rb") as i:
                if word in catalog:
                    i.seek(catalog[word][0])
                    data = pickle.load(i)
                    temp_tf = {}
                    temp_pos = {}
                    for d in data[1]:
                        # d[0]: document id, d[1]: ttf
                        temp_tf[d[0]] = d[1]
                        temp_pos[d[0]] = d[2]
                    w_d = len(temp_tf)
            # w_d, tf = get_term_statistics(word)
            wfd_collection[word] = w_d
            term_maps_collection[word] = temp_pos
            tf_collection.append(temp_tf)
        tf_for_queries[q_no] = tf_collection

def build_total_tf_wd(q_no):
    query = query_list[q_no]
    words = query.split(' ')
    total_tf_list = []
    
    for i in range(len(words)):
        total_tf_list.append(np.sum(list(tf_for_queries[q_no][i].values())))

    total_tf_wd[q_no] = total_tf_list

if __name__ == '__main__':
    """Main function
    """
    clean_results_folder()
    query_list = build_query_list()
    query_list_for_ps = build_query_list_for_prox_search()
    build_document_statistics()
    build_tf_for_queries()

    run_tf_idf()
    run_bm25() 
    run_laplace_unigram()
    
    build_term_maps_for_queries()
    run_proximity_search()
    run_laplace_unigram_with_ps()
    run_bm25_with_ps()
    run_tf_idf_with_ps()


