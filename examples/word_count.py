import re
from Graph import Graph, Input, Reduce, Map, Sort


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


def word_counter(rows):
    """ Count words. """
    yield {
        'word' : rows[0]['word'],
        'number' : len(rows)
    }


if __name__ == "__main__":

    input_node = Input()
    mapper = Map(split_text)(input_node)
    sort = Sort("word")(mapper)
    reduce = Reduce(word_counter, "word")(sort)

    graph = Graph(input_node=input_node, output_node=reduce)
    graph.run(input_file="data/text_corpus.txt",
              output_file=open("word_count.txt", "w"))
