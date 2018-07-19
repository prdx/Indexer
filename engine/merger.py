from utils.config import Config
import multiprocessing
import os
import operator
from pathlib import Path
import pickle
import uuid

class Merger(object):
    config = Config("./settings.yml")
    output_dir = config.get("output_dir")

    def run(self):
        print("Merging ...")
        workers = []
        temp_files = [name for name in os.listdir(self.output_dir) if name.endswith(".p") and "meta" not in name]
        n_workers = int(len(temp_files) / 2)

        if len(temp_files) <= 1:
            return 0
        
        for i in range(n_workers):
            first_file = temp_files.pop()
            second_file = temp_files.pop()
            t = multiprocessing.Process(target = self.__merge,
                    args = (first_file, second_file))
            workers.append(t)
            t.start()

        for w in workers:
            w.join()
        
        self.run()
        return 0

    def __merge(self, first_file, second_file):
        # Merge the meta files
        try:
            meta = {}
            
            first_file_path = self.output_dir + first_file
            second_file_path = self.output_dir + second_file

            first_meta = {}
            second_meta = {}

            with open(first_file_path + ".meta", "rb") as f1, open(second_file_path + ".meta", "rb") as f2:
                first_meta = pickle.load(f1)
                second_meta = pickle.load(f2)

            meta = self.__combine_dicts(first_meta, second_meta)
            
            # Generate new name for merged file
            file_id = str(uuid.uuid4().hex)
            
            # Generate the path
            merged_file_path = self.output_dir + "{0}.p".format(file_id)
            merged_file_meta_path = self.output_dir + "{0}.p".format(
                    file_id) + ".meta"

            merged_meta = {}
            # For each term in meta read file
            with open(merged_file_path, "wb") as f:
                for term in meta:
                    offset = f.tell()
                    inverted_list = []
                    if len(meta[term]) == 2:
                        with open(first_file_path, "rb") as f1:
                            f1.seek(meta[term][0])
                            key, value = pickle.load(f1)
                            inverted_list += value
                        with open(second_file_path, "rb") as f2:
                            f2.seek(meta[term][1])
                            key, value = pickle.load(f2)
                            inverted_list += value
                    elif len(meta[term]) == 1:
                        # If on first file
                        if term in first_meta:
                            with open(first_file_path, "rb") as f1:
                                f1.seek(meta[term][0])
                                key, value = pickle.load(f1)
                                inverted_list += value
                        else:
                            with open(second_file_path, "rb") as f2:
                                f2.seek(meta[term][0])
                                key, value = pickle.load(f2)
                                inverted_list += value
                    merged_meta[term] = [ offset ]
                    pickle.dump((term, inverted_list), f, protocol=pickle.HIGHEST_PROTOCOL)

            with open(merged_file_meta_path, "wb") as f:
                pickle.dump(merged_meta, f, protocol=pickle.HIGHEST_PROTOCOL)

            # Remove files
            for p in Path(self.output_dir).glob(first_file + "*"):
                p.unlink()
            for p in Path(self.output_dir).glob(second_file + "*"):
                p.unlink()

        except Exception as e:
            raise Exception(e)


    def __combine_dicts(self, a, b, op = operator.add):
        return { **a, **b, **{k: op(a[k], b[k]) for k in a.keys() & b}}
