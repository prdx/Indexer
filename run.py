from engine.indexer import Indexer
from engine.merger import Merger
from utils.text import gather_documents 
from utils.config import Config
from utils.serializer import Serializer
import os

config = Config("./settings.yml")
data_dir = config.get("data_dir")
output_dir = config.get("output_dir")
stats_dir = config.get("stats_dir")


def clean():
    print("Removing all files in the output folder")
    output_files = os.listdir(output_dir)
    for file_name in output_files:
        os.remove(output_dir + file_name)
    stats_files = os.listdir(stats_dir)
    for file_name in stats_files:
        os.remove(stats_dir + file_name)

if __name__ == "__main__":
    clean()
    documents = gather_documents(data_dir)
    indexer = Indexer(documents)
    indexer.run()
    merger = Merger()
    merger.run()
    serializer = Serializer()
    serializer.pickle_to_txt()
    serializer.wrap_up()
