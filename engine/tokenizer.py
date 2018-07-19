import re
from engine.stemmer import Stemmer
from engine.stats_collector import StatsCollector

class Tokenizer(object):
    """ Tokenize text
    Any number of letters and numbers, possibly separated by single
    periods in the middle. For instance, bob and 376 and 98.6 and 192.160.0.1
    are all tokens. 123,456 and aunt"s are not tokens 
    """
    __stats_collector = StatsCollector()
    __stemmer = None
    __stopwords = []

    __pattern = r"\w+(\.?\w+)*"

    def tokenize(self, doc_id, text):
        # Lower case all the body
        text = text.lower()
        regex = re.compile(self.__pattern, re.IGNORECASE | re.MULTILINE)

        tokens = []
        for match in regex.finditer(text):
            tokens.append(match.group())

        # Remove stopwords
        tokens = [ token for token in tokens if token not in self.__stopwords ]

        # If stemmed
        if self.__stemmer and self.__stemmer.method:
            tokens = [ self.__stemmer.stem(token) for token in tokens ]

        tokens = [(index + 1, word) for index, word in enumerate(tokens)]
        tokens_dict = {}
        for pos, term in tokens:
            try:
                tokens_dict[term]["tf"] += 1
                tokens_dict[term]["positions"].append(pos)
            except KeyError:
                tokens_dict[term] = { 
                        "tf": 1,
                        "positions": [pos],
                        }

        result = {
                "doc_id": doc_id,
                "tokens": tokens_dict
                }
        self.__stats_collector.write_document_length(result)

        return result


    def __init__(self, stopwords_path = None, stemmer = None):
        # If stopwords is defined
        if stopwords_path:
            try:
                with open(stopwords_path, "r") as s:
                    self.__stopwords = s.read().split("\n")
            except Exception as exception:
                print(exception)
                
        # If using stemmer
        if stemmer:
            self.__stemmer = Stemmer(stemmer)
            if self.__stemmer.method == None:
                raise KeyError("Stemming method not found")

