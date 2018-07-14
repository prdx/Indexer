from engine.catalog_writer import CatalogWriter
from engine.tokenizer import Tokenizer
from utils.text import gather_documents 
from utils.config import Config
from utils.serializer import Serializer
import multiprocessing


config = Config("./settings.yml")
output_dir = config.get("output_dir")
serializer = Serializer(output_dir)
data_dir = config.get("data_dir")


def merge_tokenized_data(tokenized_data):
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

def tokenize_in_batch(docs, counter):
    print "Worker {0} is running.".format(counter)
    tokenizer = Tokenizer('./AP_DATA/stoplist.txt', 'PorterStemmer')
    tokenized_data = [tokenizer.tokenize(doc_id, text) for (doc_id, text) in docs]
    index = merge_tokenized_data(tokenized_data)
    serializer.marshall_to_temp_objects(index, "temp.{0}.p".format(counter))
    print "Worker {0} is done.".format(counter)


def run_workers(documents, steps = 1000):
    workers = []
    n_workers = len(documents) / steps
    for i in range(n_workers):
        t = multiprocessing.Process(target = tokenize_in_batch
                , args = (documents[0: steps], i))
        workers.append(t)
        t.start()
        documents = documents[steps:]

    # If the number of docs % n is not 0, then process the remaining
    if len(documents) > 0:
        t = multiprocessing.Process(target = tokenize_in_batch,
                args = (documents, n_workers))
        workers.append(t)
        t.start()
    
    # Wait until it is done
    for w in workers:
        w.join()
    

if __name__ == "__main__":
    documents = gather_documents(data_dir)
    run_workers(documents)
