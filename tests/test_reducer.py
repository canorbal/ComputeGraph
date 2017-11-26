import pytest
from Graph import Input, Reduce, Graph

from collections import Counter


@pytest.fixture
def get_persons():
    return [
        {"name": "Andrey", "id": 1},
        {"name": "Leonid", "id": 2},
        {"name": "Sergey", "id": 1},
        {"name": "Grigoroy", "id": 4},
        {"name": "Maxim", "id": 5},
    ]


@pytest.fixture
def get_cities():
    return [
    {"id": 1, "name": "Mocsow"},
    {"id": 2, "name": "SPb"},
    {"id": 3, "name": "Kazan"},
    {"id": 7, "name": "Novgorod"},
    {"id": 10, "name": "Kaluga"},
    {"id": 12, "name": "Tula"},
]


@pytest.fixture
def get_numbers():
    return [
    {"a": 1},
    {"a": 2},
    {"a": 3},
    {"a": 4},
    {"a": 5},
]


@pytest.fixture
def get_docs_words():
    return [
        {"doc_id": 1, 'word': 'a'},
        {"doc_id": 1, 'word': 'a'},
        {"doc_id": 1, 'word': 'b'},
        {"doc_id": 1, 'word': 'c'},
        {"doc_id": 2, 'word': 'a'},
        {"doc_id": 2, 'word': 'a'},
        {"doc_id": 2, 'word': 'a'},
        {"doc_id": 2, 'word': 'd'},
        {"doc_id": 3, 'word': 'x'},
        {"doc_id": 3, 'word': 'y'},
        {"doc_id": 3, 'word': 'y'},
    ]


@pytest.fixture
def get_advanced_number():
    return [
        {"id": 1, "word": "a", "value": 1},
        {"id": 1, "word": "b", "value": 2},
        {"id": 2, "word": "b", "value": 3},
        {"id": 2, "word": "b", "value": 4},
        {"id": 2, "word": "b", "value": 5},
        {"id": 3, "word": "c", "value": 6},
        {"id": 4, "word": "c", "value": 7},
        {"id": 4, "word": "d", "value": 1},
        {"id": 4, "word": "d", "value": 4},
        {"id": 5, "word": "d", "value": -4},
    ]


def word_reducer(records):

    word_count = Counter()

    for r in records:
        word_count[r['word']] += 1

    for w, count in word_count.items():
        yield {
            'doc_id': r['doc_id'],
            'word': w,
            'count': count
        }


def test_word_reducer():

    input_node = Input(input=get_docs_words())
    reducer_node = Reduce(word_reducer, "doc_id")(input_node)

    graph = Graph(input_node=input_node, output_node=reducer_node)
    res = graph.run()

    assert res == [
        {'doc_id': 1, 'word': 'a', 'count': 2},
        {'doc_id': 1, 'word': 'b', 'count': 1},
        {'doc_id': 1, 'word': 'c', 'count': 1},
        {'doc_id': 2, 'word': 'a', 'count': 3},
        {'doc_id': 2, 'word': 'd', 'count': 1},
        {'doc_id': 3, 'word': 'x', 'count': 1},
        {'doc_id': 3, 'word': 'y', 'count': 2},
    ]


def unique_reducer(records):
    yield records[0]


def test_unique_reducer():
    input_node = Input(input=get_docs_words())
    reducer_node = Reduce(unique_reducer, ["doc_id", "word"])(input_node)

    graph = Graph(input_node=input_node, output_node=reducer_node)
    res = graph.run()

    for value in res:
        print(value)

    assert res == [
        {'doc_id': 1, 'word': 'a'},
        {'doc_id': 1, 'word': 'b'},
        {'doc_id': 1, 'word': 'c'},
        {'doc_id': 2, 'word': 'a'},
        {'doc_id': 2, 'word': 'd'},
        {'doc_id': 3, 'word': 'x'},
        {'doc_id': 3, 'word': 'y'},
    ]


def sum_reducer(records):

    sum = 0
    for r in records:
        sum += r["value"]

    yield {"id": r["id"], "word": r["word"], "sum": sum}


def test_sum_reducer():

    input_node = Input(input=get_advanced_number())
    reducer_node = Reduce(sum_reducer, ["id", "word"])(input_node)

    graph = Graph(input_node=input_node, output_node=reducer_node)

    res = graph.run()
    assert res == [
        {'id': 1, 'word': 'a', 'sum': 1},
        {'id': 1, 'word': 'b', 'sum': 2},
        {'id': 2, 'word': 'b', 'sum': 12},
        {'id': 3, 'word': 'c', 'sum': 6},
        {'id': 4, 'word': 'c', 'sum': 7},
        {'id': 4, 'word': 'd', 'sum': 5},
        {'id': 5, 'word': 'd', 'sum': -4},
    ]
