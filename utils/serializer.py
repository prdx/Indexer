import logging
import pickle

class Serializer(object):
    __output_dir = ""

    def marshall_to_temp_objects(self, index, file_name):
        file_path = self.__output_dir + file_name
        try:
            with open(file_path, 'wb') as f:
                for term in sorted(index.keys()):
                    inverted_list = index[term]
                    pickle.dump((term, inverted_list), f)

        except Exception, e:
            logging.error(" Failed to saving to pickle.\n" + str(e))

    def __init__(self, output_dir):
        self.__output_dir = output_dir
        

