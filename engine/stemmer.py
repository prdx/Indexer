from nltk.stem import PorterStemmer
from nltk.stem import SnowballStemmer

class Stemmer(object):
    method = None

    def stem(self, words):
        return self.method().stem(words)

    def __porter_stemmer(self):
        return PorterStemmer()

    def __snowball_stemmer(self):
        return SnowballStemmer()

    def __init__(self, alg = "PorterStemmer"):
        switcher = {
                "PorterStemmer": self.__porter_stemmer,
                "Snowball": self.__snowball_stemmer
                }
        
        self.method = switcher.get(alg, None)
