from engine.catalog_writer import CatalogWriter
from engine.indexer import Indexer
from engine.merger import Merger
from utils.text import gather_documents 
from utils.config import Config
from utils.serializer import Serializer
import sys

config = Config("./settings.yml")
data_dir = config.get("data_dir")
sys.setrecursionlimit(1500)


if __name__ == "__main__":
    documents = gather_documents(data_dir)
    indexer = Indexer(documents)
    indexer.run()
    merger = Merger()
    merger.run()
    serializer = Serializer()
    serializer.pickle_to_txt()
