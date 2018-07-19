from utils.config import Config
import numpy as np
import uuid

class StatsCollector(object):
    __config = Config("./settings.yml")
    __stats_dir = __config.get("stats_dir")
    __document_length_catalog = __stats_dir + "document_length.txt" 

    def write_document_length(self, document_tokens):
        filename = self.__document_length_catalog 
        with open(filename, "a") as c:
            document_length = self.__count_document_length(
                    document_tokens["tokens"])
            c.write(document_tokens["doc_id"] + \
                    "," + str(document_length) + "\n")
                

    def __count_document_length(self, tokens_dict):
        document_length = 0
        for term in tokens_dict:
            document_length += tokens_dict[term]["tf"]
        return document_length

