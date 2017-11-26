import math
import re
from Graph import Graph, Input, Reduce, Map, Sort


def docs_count(rows):
    """ Count docs in input records. """
    for row in rows:
        row.update({'number_of_docs': len(rows)})
        yield row


def split_text(row):
    """
    Split rows with 'text' field into set of rows with 'token' field
    (one for every occurrence of every word in text if word is acceptable).
    """
    new_text = re.sub('[^A-Za-z]+', ' ', row['text'])
    tokens = new_text.split()
    for token in tokens:
        if len(token) > 4:
            yield {
                'doc_id': row['doc_id'],
                'number_of_docs': row['number_of_docs'],
                'word': token.lower()
            }


def count_words_in_doc(rows):
    """ Count words in doc. """
    rows[0].update({"number_in_doc": len(rows)})
    yield rows[0]


def count_sum_of_docs(rows):
    """ Count sum of docs. """
    flag = True
    if len(rows) < rows[0]['number_of_docs']:
        flag = False

    for row in rows:
        if row['number_in_doc'] < 2:
            flag = False

    if flag:
        sum_of_docs = sum(row['number_in_doc'] for row in rows)
        for row in rows:
            row.update({"sum_of_docs": sum_of_docs})
            yield row


def count_words_in_one_doc(rows):
    """ Count number of word occurrence in one doc. """
    number = sum(row['number_in_doc'] for row in rows)
    for row in rows:
        row.update({"words_in_doc": number})
        yield row


def count_words(rows):
    """ Count total number of word occurrence in all docs. """
    number = sum(row['words_in_doc'] for row in rows)
    for row in rows:
        row.update({"words_in_total": number})
        yield row


def count_pmi(rows):
    """ Calculate PMI and yield result. """
    words = []
    for row in rows:
        pmi = math.log(row['number_in_doc'] * row['words_in_total']
                       / row['sum_of_docs'] / row['words_in_doc'])

        words.append((row['word'], pmi))

    words.sort(key=lambda x: x[1], reverse=True)
    yield {
        'doc_id': rows[0]['doc_id'],
        'top_words': words[:10]
    }


if __name__ == "__main__":

    input_node = Input()
    docs_count_reducer = Reduce(docs_count)(input_node)
    split_mapper = Map(split_text, "tokenizer")(docs_count_reducer)
    sort_by_word_doc_id = Sort(by=['word', 'doc_id'])(split_mapper)

    words_in_doc_reducer = Reduce(count_words_in_doc, key=('word',
                                              'doc_id'))(sort_by_word_doc_id)

    sum_of_docs = Reduce(count_sum_of_docs,
                         key=('word'))(words_in_doc_reducer)

    sort_by_doc_id = Sort(by="doc_id")(sum_of_docs)

    word_in_one_doc_reducer = Reduce(count_words_in_one_doc,
                                     key="doc_id")(sort_by_doc_id)

    words_reducer = Reduce(count_words)(word_in_one_doc_reducer)
    pmi_reducer = Reduce(count_pmi, "doc_id")(words_reducer)

    count_pmi = Graph(input_node=input_node, output_node=pmi_reducer)

    res = count_pmi.run(input_file="data/text_corpus.txt",
                        output_file=open("pmi.txt", "w"))