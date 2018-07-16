from utils.config import Config
import numpy as np

class CatalogWriter(object):
    config = Config("./settings.yml")
    catalogs_dir = config.get("catalogs_dir")
    __document_length_catalog = catalogs_dir + "document_length.txt" 

    def write_document_length(self, document_tokens):
        try:
            with open(self.__document_length_catalog, "a") as c:
                document_length = self.__count_document_length(
                        document_tokens["tokens"])
                c.write(document_tokens["doc_id"] + \
                        "," + str(document_length) + "\n")
        except Exception as exception:
            print exception
                

    def __count_document_length(self, tokens_dict):
        document_length = 0
        for term in tokens_dict:
            document_length += tokens_dict[term]["tf"]
        return document_length

