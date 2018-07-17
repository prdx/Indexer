import pickle

class IndexStatistics(object):
    doc_count = 0
    avg_doc_length = 0
    vocab_size = 0

    def __init__(self):
        with open("./index/document_length.txt", "r") as stats:
            data = stats.read()
        data = data.split("\n")
        sum_ttf = 0
        for d in data:
            try:
                doc_stats = d.split(",")
                sum_ttf += int(doc_stats[1])
            except IndexError:
                break
        self.doc_count = len(data)
        self.avg_doc_length = sum_ttf / self.doc_count 
        with open("./index/index.p.meta", "rb") as m:
            m = pickle.load(m)
            self.vocab_size = len(m) 

