import re
from stemmer import Stemmer

class Tokenizer(object):
    """ Tokenize text
    Any number of letters and numbers, possibly separated by single
    periods in the middle. For instance, bob and 376 and 98.6 and 192.160.0.1
    are all tokens. 123,456 and aunt"s are not tokens 
    """
    __stemmer = None
    __stopwords = []

    __pattern = r"\w+(\.?\w+)*"

    def tokenize(self, text):
        # Lower case all the body
        tokens = text.lower()
        tokens = [ match.group() for match in re.finditer(self.__pattern, text, re.M | re.I) ]

        # Give ID to the word and remove stopwords
        tokens = [ token for token in tokens if token not in self.__stopwords ]

        # If stemmed
        if self.__stemmer and self.__stemmer.method:
            tokens = [ self.__stemmer.stem(token) for token in tokens ]
        
        return tokens


    def __init__(self, stopwords_path = None, stemmer = None):
        # If stopwords is defined
        if stopwords_path:
            try:
                with open(stopwords_path, "r") as s:
                    self.__stopwords = s.read().split("\n")
            except Exception as exception:
                print exception
                
        # If using stemmer
        if stemmer:
            self.__stemmer = Stemmer(stemmer)
            if self.__stemmer.method == None:
                raise KeyError("Stemming method not found")
