import pytest
from Graph import Input, Map, Graph


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


def simple_mapper(row):
    yield row


def test_simple_mapper():
    input_node = Input(input=get_numbers())
    mapper_node = Map(simple_mapper)(input_node)

    assert mapper_node.input == input_node
    graph = Graph(input_node=input_node, output_node=mapper_node)
    res = graph.run()
    assert res == get_numbers()


def test_empty_mapper():
    input_node = Input(input=[])
    mapper_node = Map(simple_mapper)(input_node)
    graph = Graph(input_node=input_node, output_node=mapper_node)
    res = graph.run()
    assert res == []


def person_mapper(row):
    if row['id'] < 3:
        yield row


def test_person_mapper():
    input_node = Input(input=get_persons())
    mapper_node = Map(person_mapper)(input_node)
    graph = Graph(input_node=input_node, output_node=mapper_node)
    res = graph.run()
    assert res == [
        {"name": "Andrey", "id": 1},
        {"name": "Leonid", "id": 2},
        {"name": "Sergey", "id": 1},
    ]


def square_mapper(row):
    yield {'a': row['a']**2}


def test_square_mapper():
    input_node = Input(input=get_numbers())
    mapper_node = Map(square_mapper)(input_node)
    graph = Graph(input_node=input_node, output_node=mapper_node)
    res = graph.run()

    answer = [{'a': i**2} for i in range(1, 6)]
    assert res == answer
