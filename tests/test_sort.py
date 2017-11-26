import pytest
from Graph import Input, Sort, Graph


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
def get_advanced_persons():
    return [
    {"name": "Andrey", "id": 1, "age": 38},
    {"name": "Leonid", "id": 2, "age": 20},
    {"name": "Sergey", "id": 1, "age": 25},
    {"name": "Grigoroy", "id": 4, "age": 64},
    {"name": "Misha", "id": 1, "age": 5},
    {"name": "Roma", "id": 1, "age": 10},
    {"name": "Rishat", "id": 2, "age": 17},
    {"name": "Maxim", "id": 5, "age": 28},
    {"name": "Stepan", "id": 10, "age": 14},
]


def test_simple_sort():
    input_node = Input(input=get_numbers()[::-1])
    sort_node = Sort(by='a')(input_node)

    graph = Graph(input_node=input_node, output_node=sort_node)
    res = graph.run()
    assert res == get_numbers()


def test_persons_sort_name():
    input_node = Input(input=get_advanced_persons())
    sort_node = Sort(by='name')(input_node)
    graph = Graph(input_node=input_node, output_node=sort_node)
    res = graph.run()
    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'name': 'Andrey', 'id': 1, 'age': 38},
        {'name': 'Grigoroy', 'id': 4, 'age': 64},
        {'name': 'Leonid', 'id': 2, 'age': 20},
        {'name': 'Maxim', 'id': 5, 'age': 28},
        {'name': 'Misha', 'id': 1, 'age': 5},
        {'name': 'Rishat', 'id': 2, 'age': 17},
        {'name': 'Roma', 'id': 1, 'age': 10},
        {'name': 'Sergey', 'id': 1, 'age': 25},
        {'name': 'Stepan', 'id': 10, 'age': 14},
    ]


def test_persons_sort_id_name():
    input_node = Input(input=get_advanced_persons())
    sort_node = Sort(by=['id', 'name'])(input_node)
    graph = Graph(input_node=input_node, output_node=sort_node)
    res = graph.run()
    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'name': 'Andrey', 'id': 1, 'age': 38},
        {'name': 'Misha', 'id': 1, 'age': 5},
        {'name': 'Roma', 'id': 1, 'age': 10},
        {'name': 'Sergey', 'id': 1, 'age': 25},
        {'name': 'Leonid', 'id': 2, 'age': 20},
        {'name': 'Rishat', 'id': 2, 'age': 17},
        {'name': 'Grigoroy', 'id': 4, 'age': 64},
        {'name': 'Maxim', 'id': 5, 'age': 28},
        {'name': 'Stepan', 'id': 10, 'age': 14},

    ]


def test_persons_sort_name_age():
    input_node = Input(input=get_advanced_persons())
    sort_node = Sort(by=['age', 'name'])(input_node)
    graph = Graph(input_node=input_node, output_node=sort_node)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'name': 'Misha', 'id': 1, 'age': 5},
        {'name': 'Roma', 'id': 1, 'age': 10},
        {'name': 'Stepan', 'id': 10, 'age': 14},
        {'name': 'Rishat', 'id': 2, 'age': 17},
        {'name': 'Leonid', 'id': 2, 'age': 20},
        {'name': 'Sergey', 'id': 1, 'age': 25},
        {'name': 'Maxim', 'id': 5, 'age': 28},
        {'name': 'Andrey', 'id': 1, 'age': 38},
        {'name': 'Grigoroy', 'id': 4, 'age': 64},
    ]


