from collections import Counter
from operator import itemgetter
import math
import re
from Graph import Graph, Input, Join, Fold, Reduce, Map, Sort


def split_text(record):
    """
    Split rows with 'text' field into set of rows with 'token' field
    (one for every occurence of every word in text)
    """
    new_text = re.sub('[^A-Za-z]+', ' ', record['text'])
    tokens = new_text.split()
    for token in tokens:
        yield {
            'doc_id': record['doc_id'],
            'word': token.lower(),
        }


def docs_count(state, record):
    """ Increment of docs_count """
    return {"docs_count": state["docs_count"] + 1}


def unique(records):
    """ Convolve records with similar key to one record. """
    yield records[0]


def calc_idf(records):
    """ Calculate idf. """
    doc_counter = Counter()
    for row in records:
        doc_counter[row['word']] += 1

    for w, count in doc_counter.items():
        yield {
            "word": row["word"],
            "count_idf": count,
            "docs_count": row["docs_count"]
        }


def term_frequency_reducer(records):
    """ Calculate term frequency for every word in doc_id. """
    word_count = Counter()
    for row in records:
        word_count[row['word']] += 1

    total = sum(word_count.values())

    for w, count in word_count.items():
        yield {
            'doc_id': row['doc_id'],
            'word': w,
            'tf': count / total
        }


def invert_index(records):
    """ Calculate final result. """
    for row in records:
        row["tf_idf"] = row["tf"] * \
                        math.log(row['docs_count'] / row['count_idf'])

    records = sorted(records, key = itemgetter("tf_idf"), reverse=True)

    yield {
        "word": row["word"],
        "index": [(records[i]["doc_id"], records[i]["tf_idf"])
                  for i in range(0, min(3, len(records)))]
    }


if __name__ == "__main__":

    split_input_node = Input()
    split_mapper = Map(split_text)(split_input_node)
    split_words = Graph(input_node=split_input_node, output_node=split_mapper,
                        name="split_words")

    fold_input = Input()
    folder = Fold(docs_count, {"docs_count": 0}, "doc_number")(fold_input)
    count_docs = Graph(input_node=fold_input, output_node=folder)

    count_idf_input = Input(split_words)
    sort_node = Sort(["doc_id", "word"])(count_idf_input)
    reducer = Reduce(unique, ["doc_id", "word"])(sort_node)
    join = Join(count_docs, [], "outer")(reducer)
    sort_by_word = Sort("word")(join)
    count_idf_reducer = Reduce(calc_idf, ["word"])(sort_by_word)
    count_idf = Graph(input_node=count_idf_input,
                      output_node=count_idf_reducer)

    calc_index_input = Input(split_words)
    sort_doc = Sort("doc_id")(calc_index_input)
    tf_reducer = Reduce(term_frequency_reducer, "doc_id")(sort_doc)
    join_left = Join(count_idf, "word", "left")(tf_reducer)
    invert_reduce = Reduce(invert_index, "word")(join_left)
    calc_index = Graph(input_node=calc_index_input, output_node=invert_reduce)

    dependencies = {
        split_words: "data/text_corpus.txt",
        count_docs: "data/text_corpus.txt",
    }

    res = calc_index.run(inputs=dependencies,
                         output_file=open("tf-idf.txt", "w"))
