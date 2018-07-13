import numpy as np

class CatalogWriter(object):
    __document_length_catalog = ''

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

    def __init__(self, catalogs_paths):
        self.__document_length_catalog = catalogs_paths + "/document_length.txt"
