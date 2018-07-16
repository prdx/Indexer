import os
import re

def find_docs_by_regex(text):
    doc_regex = re.compile("<DOC>.*?</DOC>", re.DOTALL)
    result = re.findall(doc_regex, text)
    return result

def find_doc_no_by_regex(text):
    docno_regex = re.compile("<DOCNO>.*?</DOCNO>")
    result = re.findall(docno_regex, text)[0] \
                    .replace("<DOCNO>", "") \
                    .replace("</DOCNO>", "") \
                    .strip()
    return result

def find_all_texts_by_regex(text):
    text_regex = re.compile("<TEXT>.*?</TEXT>", re.DOTALL)
    result = "".join(re.findall(text_regex, text)) \
            .replace("<TEXT>", "")  \
            .replace("</TEXT>", "") \
            .replace("\n", " ")
    return result

def gather_documents(path):
    documents = []
    for f in os.listdir(path):
        if f.startswith('ap'):
            with open(path + f, 'r', encoding='ISO-8859-1') as d:
                d = d.read()
                docs = find_docs_by_regex(d)

                for doc in docs:
                    doc_id = find_doc_no_by_regex(doc)
                    text = find_all_texts_by_regex(doc)
                    text = text.strip()

                    documents.append((doc_id, text))
    return documents


