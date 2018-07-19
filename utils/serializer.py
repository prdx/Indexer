from utils.config import Config
import json
import logging
import os
import pickle
import uuid

class Serializer(object):
    config = Config("./settings.yml")
    __output_dir = config.get("output_dir")

    def marshall_to_temp_objects(self, index):
        file_name = str(uuid.uuid4().hex) + ".p"
        file_path = self.__output_dir + file_name
        meta_file_path = self.__output_dir + file_name + ".meta"
        meta = {}
        try:
            with open(file_path, 'wb') as f:
                for term in sorted(index.keys()):
                    offset = f.tell()
                    meta[term] = [ offset ]
                    inverted_list = index[term]
                    pickle.dump((term, inverted_list), f, protocol=pickle.HIGHEST_PROTOCOL)

            with open(meta_file_path, 'wb') as f:
                pickle.dump(meta, f, protocol=pickle.HIGHEST_PROTOCOL)

        except Exception as e:
            logging.error(" Failed to saving to pickle.\n" + str(e))

    def pickle_to_txt(self):
        pickle_files = [name for name in os.listdir(self.__output_dir) if name.endswith(".p") and "meta" not in name]
        for pickle_file in pickle_files:
            try:
                with open(self.__output_dir + pickle_file, "rb") as p, open(self.__output_dir + pickle_file + ".txt", "w") as t:
                    while True:
                        term, value = pickle.load(p)
                        print(value)
                        t.write(term + " " + json.dumps(value) + "\n")
            except EOFError: return
