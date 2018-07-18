import numpy as np

from utils.statistics import IndexStatistics
from utils.text import * 

class ProximitySearchModel(object):
    # Constants
    k1 = 1.2
    k2 = 100
    b = 0.75
    document_statistics = {}

    def query(self, keywords, term_maps_collection, wd_collection = [], tf_collection = []):
        tf = 0
        result = {}

        words = keywords.split(' ')

        print("Calculating the okapi BM25")
        print(keywords)
        
        file_list = get_file_list()
        for doc in file_list:
            tf = 0
            for i in range(len(words)):
                if doc in tf_collection[i]:
                    tf +=  tf_collection[i][doc]

            term_maps = self.__build_term_maps(term_maps_collection, keywords, doc)
            min_span = self.__find_minimum_span(term_maps)
            # print(min_span)
            if min_span == 0:
                continue
            result[doc] = (1500 - min_span) * (tf / (self.document_statistics[doc] + self.index_statistics.vocab_size))
            
        return result
    
    def __phi(self, distance):
        alpha = 1
        if distance == 0:
            return 1
        return np.log(alpha + np.exp(-1.0 * distance))


    def __bm25(self, doc_no = '', df_w = 0, tf_wd = 0):
        """
        """
        D = self.index_statistics.doc_count
        doc_length = self.document_statistics[doc_no]
        avg_doc_length = self.index_statistics.avg_doc_length
        d = doc_length / float(avg_doc_length)

        first_term = (D + 0.5) / (df_w + 0.5)
        first_term = np.log(first_term)

        second_term_numerator = tf_wd + self.k1 * tf_wd
        second_term_denominator = tf_wd + self.k1 * ((1 - self.b) + self.b * d)
        second_term = second_term_numerator / second_term_denominator

        # We consider the tf_wq = 1, thus we can ignore the third term
        
        return first_term * second_term

    def __build_term_maps(self, term_maps_collection, keywords, doc_id):
        term_maps = {}
        words = keywords.split(' ')

        for word in words:
            # If a word does not appear anywhere
            if word in term_maps_collection:
                word_position = term_maps_collection[word]
            else: 
                term_maps[word] = [4 * self.document_statistics[doc_id] + 1]
                continue
    
            if doc_id not in word_position:
                # If the word does not appear anywhere
                term_maps[word] = [4 * self.document_statistics[doc_id] + 1]
                continue
            term_maps[word] = word_position[doc_id]
        return term_maps



    def __find_minimum_span(self, term_maps):
        # term_maps = {
            # "term1": [0, 5, 10, 15],
            # "term2": [1, 3, 6, 9],
            # "term3": [4, 8, 16, 21]
        # }


        window_index = dict(zip(term_maps.keys(), [0] * len(term_maps))) 
        # print("Window index:")
        # print(window_index)

        current_values = self.__build_current_values(term_maps, window_index)
        min_current_key = min(current_values, key=current_values.get)
        max_current_key = max(current_values, key=current_values.get)
        minimum_span = current_values[max_current_key] - current_values[min_current_key]
        

        # print("Current values:")
        # print(current_values)

        while self.__is_window_end(window_index, term_maps) != True:
            # Update the minimum span
            min_current_key = min(current_values, key=current_values.get)
            max_current_key = max(current_values, key=current_values.get)
            delta = current_values[max_current_key] - current_values[min_current_key]
            if delta < minimum_span:
                minimum_span = delta

            # Find the lowest index
            advanced_term = self.__find_next_advanced_index(term_maps, window_index)
            # Advance the lowest index
            window_index[advanced_term] += 1

            # print("Window index:")
            # print(window_index)

            # Rebuild the current values
            current_values = self.__build_current_values(term_maps, window_index)

        return minimum_span

    def __build_current_values(self, term_maps, window_index):
        current_values = {}
        for term in term_maps:
            try:
                current_values[term] = int(term_maps[term][window_index[term]])
            except:
                print(term)

        return current_values

    
    def __is_window_end(self, window_index, term_maps):
        end = [ len(term_maps[key]) - 1 for key in term_maps ]
        is_window_end = False

        i = 0
        for term in window_index:
            if window_index[term] >= end[i]:
                is_window_end = True
            else:
                is_window_end = False
            i += 1

        return is_window_end

    def __find_next_advanced_index(self, term_maps, window_index):
        """
        """
        _term_maps = term_maps
        _window_index = window_index
        advanced_term = min(_window_index, key=_window_index.get)
        removed_keys = []
        # If the term has reached the latest position
        while _window_index[advanced_term] + 1 == len(_term_maps[advanced_term]):
            removed_keys.append(advanced_term)
            _window_index = {term: _window_index[term] for term in _window_index if term not in removed_keys}
            advanced_term = min(_window_index, key=_window_index.get)
        return advanced_term

    def __init__(self, document_statistics):
        self.index_statistics = IndexStatistics()
        self.document_statistics = document_statistics
    



# ps = ProximitySearchModel()
# ps.find_minimum_span()
