from utils.config import Config
import logging
import pickle

class Serializer(object):
    config = Config("./settings.yml")
    __output_dir = config.get("output_dir")

    def marshall_to_temp_objects(self, index, file_name):
        file_path = self.__output_dir + file_name
        meta_file_path = self.__output_dir + file_name + ".meta"
        meta = {}
        try:
            with open(file_path, 'wb') as f:
                for term in sorted(index.keys()):
                    offset = f.tell()
                    meta[term] = [ offset ]
                    inverted_list = index[term]
                    pickle.dump((term, inverted_list), f)

            with open(meta_file_path, 'wb') as f:
                pickle.dump(meta, f)

        except Exception, e:
            logging.error(" Failed to saving to pickle.\n" + str(e))

