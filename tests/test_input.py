import pytest
from Graph import Input, Graph


@pytest.fixture
def get_persons():
    return [
        {"name": "Andrey", "id": 1},
        {"name": "Leonid", "id": 2},
        {"name": "Sergey", "id": 1},
        {"name": "Grigoroy", "id": 4},
        {"name": "Maxim", "id": 5},
    ]


def test_input_node_init():
    input_node = Input(input=get_persons(), name="main_input", )
    assert input_node.name == "main_input"
    assert input_node.input == get_persons()
    assert input_node.output is None


def test_input_node_empty_run():
    input_node = Input(input=[])
    res = list(input_node.run())
    assert res == []


def test_input_node_run():
    input_node = Input(input=get_persons())
    res = list(input_node.run())
    assert res == get_persons()


def test_input_node_to_graph():
    input_node = Input(input=get_persons(), name="main_input", )
    graph = Graph(input_node=input_node, output_node=input_node,
                  name="main_graph")

    assert graph.name == "main_graph"
    assert graph.nodes == [input_node]
    assert graph._dependencies == []
    assert graph.order == []

    result = graph.run()
    assert result == get_persons()


def test_input_node_to_graph_in():
    first_input = Input(input=get_persons(), name="main_input")
    first_graph = Graph(input_node=first_input, output_node=first_input,
                        name="main_graph")

    second_input = Input(input=first_graph)
    assert second_input.input_graph == first_graph
    assert second_input.input is None

    second_graph = Graph(input_node=second_input, output_node=second_input)
    assert second_graph.order == [first_graph]


def test_order_computation():
    first = Input(input=[])
    gr1 = Graph(input_node=first, output_node=first, name="first")

    second = Input(input=gr1)
    gr2 = Graph(input_node=second, output_node=second, name="second")

    third = Input(input=gr2)
    gr3 = Graph(input_node=third, output_node=third, name="third")
    assert gr3.order == [gr1, gr2]
