from utils.config import Config
import json
import logging
import os
import pickle
import uuid
import gzip

class Serializer(object):
    config = Config("./settings.yml")
    __output_dir = config.get("output_dir")
    __stats_dir = config.get("stats_dir")

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
                        string = term + ":"
                        for v in value:
                            string += str(v[0]) + "," + str(v[1]) 
                            # position
                            for pos in v[2]:
                                string += "," + str(pos)
                            string += "|"
                            
                        t.write(string + "\n")
            except EOFError: return

    # FIXME: Bad design
    def generate_doc_id(self):
        doc_dict = {}
        with open(self.__stats_dir + "document_length.txt", "r") as stats:
            data = stats.read()
            data = data.split("\n")
            sum_ttf = 0
            doc_id = 1
            for d in data:
                doc_stats = d.split(",")
                try:
                    doc_dict[doc_stats[0]] = doc_id
                    doc_id += 1
                except IndexError:
                    break

        with open(self.__output_dir + "document_map", 'wb') as f:
            pickle.dump(doc_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
            
        return doc_dict


    def wrap_up(self):
        pickle_file = [name for name in os.listdir(self.__output_dir) if name.endswith(".p") and "meta" not in name]
        pickle_file = pickle_file[0]
        meta = {}
        final_file = open(self.__output_dir + "index", "wb")
        final_meta_file = open(self.__output_dir + "index.meta", "wb")
        term_id = 1

        doc_map = self.generate_doc_id()

        try:
            with open(self.__output_dir + pickle_file, "rb") as p:
                while True:
                    term, value = pickle.load(p)
                    string = ""
                    for v in value:
                        string += str(doc_map[v[0]]) + "," + str(v[1]) + ":"
                        # position
                        for i in range(len(v[2])):
                            string += str(v[2][i])
                            if i < len(v[2]) - 1:
                                string += ","

                        string += ";"
                    string_byte = bytes(string, "utf-8")
                    current_pos = final_file.tell()
                    string_length = len(string.encode())
                    meta[term] = [term_id, current_pos, string_length]
                    final_file.write(string.encode())
                    term_id += 1
        except EOFError: 
            final_file.close()
            pickle.dump(meta, final_meta_file, protocol=pickle.HIGHEST_PROTOCOL)
            final_meta_file.close()


