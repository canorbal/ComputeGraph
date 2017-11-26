import pytest
from Graph import Input, Fold, Graph


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


def simple_folder(state, record):
    if len(state['a']) % 2 == 0:
        state['a'].append(record['a'])
    else:
        state['a'].append(-record['a'])

    return state


def test_simple_fold():

    input_node = Input(input=get_numbers())
    folder_node = Fold(simple_folder, {"a": []})(input_node)

    graph = Graph(input_node=input_node, output_node=folder_node)
    res = graph.run()
    assert res == [{'a': [1, -2, 3, -4, 5]}]


def city_folder(state, record):
    state['name'] += record['name']
    return state


def test_city_fold():
    input_node = Input(input=get_cities())
    state = {"id": -1, "name": ""}
    folder_node = Fold(city_folder, state)(input_node)

    graph = Graph(input_node=input_node, output_node=folder_node)
    res = graph.run()
    print("***** RESULT *****")
    for value in res:
        print(value)

    print()
    print("******************")
    answer_name = ""
    names = [city["name"] for city in get_cities()]
    for name in names:
        answer_name += name

    assert res == [{"id": -1, "name": answer_name}]
