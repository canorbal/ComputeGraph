import pytest
from Graph import Input, Join, Graph


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
def get_advanced_cities():
    return [
    {"id": 1, "city": "Mocsow"},
    {"id": 2, "city": "SPb"},
    {"id": 3, "city": "Kazan"},
    {"id": 7, "city": "Novgorod"},
    {"id": 10, "city": "Kaluga"},
    {"id": 12, "city": "Tula"},
]


def test_inner_simple_join():

    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_advanced_cities())
    inner_join = Join(left_graph, 'id', "inner")(right_input)

    graph = Graph(input_node=right_input, output_node=inner_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'city': 'Mocsow', 'id': 1, 'name': 'Andrey'},
        {'age': 25, 'city': 'Mocsow', 'id': 1, 'name': 'Sergey'},
        {'age': 5, 'city': 'Mocsow', 'id': 1, 'name': 'Misha'},
        {'age': 10, 'city': 'Mocsow', 'id': 1, 'name': 'Roma'},
        {'age': 20, 'city': 'SPb', 'id': 2, 'name': 'Leonid'},
        {'age': 17, 'city': 'SPb', 'id': 2, 'name': 'Rishat'},
        {'age': 14, 'city': 'Kaluga', 'id': 10, 'name': 'Stepan'},
    ]


def test_inner_common_cols_join():

    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_cities())
    inner_join = Join(left_graph, 'id', "inner")(right_input)

    graph = Graph(input_node=right_input, output_node=inner_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'id': 1, 'left_name': 'Andrey', 'right_name': 'Mocsow'},
        {'age': 25, 'id': 1, 'left_name': 'Sergey', 'right_name': 'Mocsow'},
        {'age': 5, 'id': 1, 'left_name': 'Misha', 'right_name': 'Mocsow'},
        {'age': 10, 'id': 1, 'left_name': 'Roma', 'right_name': 'Mocsow'},
        {'age': 20, 'id': 2, 'left_name': 'Leonid', 'right_name': 'SPb'},
        {'age': 17, 'id': 2, 'left_name': 'Rishat', 'right_name': 'SPb'},
        {'age': 14, 'id': 10, 'left_name': 'Stepan', 'right_name': 'Kaluga'},
    ]


def test_left_simple_join():

    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_advanced_cities())
    left_join = Join(left_graph, 'id', "left")(right_input)

    graph = Graph(input_node=right_input, output_node=left_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'city': 'Mocsow', 'id': 1, 'name': 'Andrey'},
        {'age': 25, 'city': 'Mocsow', 'id': 1, 'name': 'Sergey'},
        {'age': 5, 'city': 'Mocsow', 'id': 1, 'name': 'Misha'},
        {'age': 10, 'city': 'Mocsow', 'id': 1, 'name': 'Roma'},
        {'age': 20, 'city': 'SPb', 'id': 2, 'name': 'Leonid'},
        {'age': 17, 'city': 'SPb', 'id': 2, 'name': 'Rishat'},
        {'age': 64, 'city': None, 'id': 4, 'name': 'Grigoroy'},
        {'age': 28, 'city': None, 'id': 5, 'name': 'Maxim'},
        {'age': 14, 'city': 'Kaluga', 'id': 10, 'name': 'Stepan'},
    ]


def test_left_common_cols_join():

    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_cities())
    left_join = Join(left_graph, 'id', "left")(right_input)

    graph = Graph(input_node=right_input, output_node=left_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'id': 1, 'left_name': 'Andrey', 'right_name': 'Mocsow'},
        {'age': 25, 'id': 1, 'left_name': 'Sergey', 'right_name': 'Mocsow'},
        {'age': 5, 'id': 1, 'left_name': 'Misha', 'right_name': 'Mocsow'},
        {'age': 10, 'id': 1, 'left_name': 'Roma', 'right_name': 'Mocsow'},
        {'age': 20, 'id': 2, 'left_name': 'Leonid', 'right_name': 'SPb'},
        {'age': 17, 'id': 2, 'left_name': 'Rishat', 'right_name': 'SPb'},
        {'age': 64, 'id': 4, 'left_name': 'Grigoroy', 'right_name': None},
        {'age': 28, 'id': 5, 'left_name': 'Maxim', 'right_name': None},
        {'age': 14, 'id': 10, 'left_name': 'Stepan', 'right_name': 'Kaluga'},
    ]


def test_right_simple_join():
    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_advanced_cities())
    right_join = Join(left_graph, 'id', "right")(right_input)

    graph = Graph(input_node=right_input, output_node=right_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'city': 'Mocsow', 'id': 1, 'name': 'Andrey'},
        {'age': 25, 'city': 'Mocsow', 'id': 1, 'name': 'Sergey'},
        {'age': 5, 'city': 'Mocsow', 'id': 1, 'name': 'Misha'},
        {'age': 10, 'city': 'Mocsow', 'id': 1, 'name': 'Roma'},
        {'age': 20, 'city': 'SPb', 'id': 2, 'name': 'Leonid'},
        {'age': 17, 'city': 'SPb', 'id': 2, 'name': 'Rishat'},
        {'age': None, 'city': 'Kazan', 'id': 3, 'name': None},
        {'age': None, 'city': 'Novgorod', 'id': 7, 'name': None},
        {'age': 14, 'city': 'Kaluga', 'id': 10, 'name': 'Stepan'},
        {'age': None, 'city': 'Tula', 'id': 12, 'name': None},
    ]


def test_outer_join():
    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_advanced_cities())
    outer_join = Join(left_graph, 'id', "outer")(right_input)

    graph = Graph(input_node=right_input, output_node=outer_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")

    assert res == [
        {'age': 38, 'city': 'Mocsow', 'id': 1, 'name': 'Andrey'},
        {'age': 25, 'city': 'Mocsow', 'id': 1, 'name': 'Sergey'},
        {'age': 5, 'city': 'Mocsow', 'id': 1, 'name': 'Misha'},
        {'age': 10, 'city': 'Mocsow', 'id': 1, 'name': 'Roma'},
        {'age': 20, 'city': 'SPb', 'id': 2, 'name': 'Leonid'},
        {'age': 17, 'city': 'SPb', 'id': 2, 'name': 'Rishat'},
        {'age': None, 'city': 'Kazan', 'id': 3, 'name': None},
        {'age': 64, 'city': None, 'id': 4, 'name': 'Grigoroy'},
        {'age': 28, 'city': None, 'id': 5, 'name': 'Maxim'},
        {'age': None, 'city': 'Novgorod', 'id': 7, 'name': None},
        {'age': 14, 'city': 'Kaluga', 'id': 10, 'name': 'Stepan'},
        {'age': None, 'city': 'Tula', 'id': 12, 'name': None},
    ]


def test_outer_without_key():
    left_input = Input(input=get_advanced_persons())
    left_graph = Graph(input_node=left_input, output_node=left_input)

    right_input = Input(input=get_advanced_cities())
    outer_join = Join(left_graph, [], "outer")(right_input)

    graph = Graph(input_node=right_input, output_node=outer_join)
    res = graph.run()

    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")