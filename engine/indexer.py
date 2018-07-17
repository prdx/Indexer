from engine.tokenizer import Tokenizer
from engine.stats_collector import StatsCollector
from utils.config import Config
from utils.serializer import Serializer
import multiprocessing
import uuid

class Indexer(object):
    config = Config("./settings.yml")
    documents = {}
    serializer = None
    stemmer = config.get("stemming")
    steps = config.get("steps")
    stoplist_path = config.get("stopwords")
    tokenizer = None
    
    def run(self):
        workers = []
        n_workers = int(len(self.documents) / self.steps)
        for i in range(n_workers):
            t = multiprocessing.Process(target = self.__tokenize_in_batch
                    , args = (self.documents[0: self.steps], i))
            workers.append(t)
            t.start()
            self.documents = self.documents[self.steps:]

        # If the number of docs % n is not 0, then process the remaining
        if len(self.documents) > 0:
            t = multiprocessing.Process(target = self.__tokenize_in_batch,
                    args = (self.documents, n_workers))
            workers.append(t)
            t.start()
        
        # Wait until it is done
        for w in workers:
            w.join()

    def __tokenize_in_batch(self, docs, counter):
        print("Worker {0} is running.".format(counter))
        tokenized_data = [self.tokenizer.tokenize(doc_id, text) for (doc_id, text) in docs]
        index = self.__merge_tokenized_data(tokenized_data)
        file_id = str(uuid.uuid4().hex)
        self.serializer.marshall_to_temp_objects(index, "temp.{0}.p".format(file_id))
        print("Worker {0} is done.".format(counter))

    def __merge_tokenized_data(self, tokenized_data):
        merged_dict = {}
        for data in tokenized_data:
            docid = data["doc_id"]
            for token, tokdata in data["tokens"].items():
                e = (docid, tokdata["tf"], tokdata["positions"])
                if token in merged_dict:
                    merged_dict[token].append(e)
                else:
                    merged_dict[token] = [e]
        return merged_dict

    def __init__(self, documents):
        self.documents =  documents
        self.serializer = Serializer()
        self.tokenizer = Tokenizer(self.stoplist_path, self.stemmer)
