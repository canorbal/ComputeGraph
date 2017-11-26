import json
from operator import itemgetter


class Node(object):
    """ Class of Graph node objects. """

    def __init__(self, input=None, output=None, name=None):
        """
        :param input: input Node object, list or Graph object.
        :param output: output Node object.
        :param name: name of this Node object.
        """
        self.input = input
        self.output = output
        self.name = name

    def __call__(self, input=None):
        """
        Connect self Node objects with input Node object in keras-like style.

        Example:
            first_node = Node(name="first_node")
            second_node = Node(name="second_node")(first_node)
            first_node.output
            >>> second_node
            second_node.input
            >>> first_node

        :param input: input Node object
        :return: Node object
        """
        input.output = self
        self.input = input
        return self

    def __str__(self):
        if self.name is None:
            return "Node id = {}".format(id(self))
        else:
            return "Node name = {}".format(self.name)


class Input(Node):
    """
    Input node is a node which yields data from source. Source
    can be a list of dicts, another Graph object or path to file.
    """

    def __init__(self, input=None, output=None, input_file=None, name=None):
        """
        :param input: list of dicts or Graph object
        :param output: Node object which is output.input == self
        :param input_file: path to file with data.
        Using only when input is None.

        :param name: name of current Node object.
        """
        super().__init__(input=input, output=output, name=name)
        self.input_file = input_file

        if isinstance(input, Graph):
            self.input_graph = input
            self.input = None
        else:
            self.input_graph = None

    def run(self):
        """
        Yield values from input source.

        If Graph object is passed as input, values will be yielded from result
        of input Graph which is already calculated (check topological sort).

        If input is a list of dicts, values will be yielded from that list.

        If input is None then input_file will be opened and values will be
        yielded from input_file.

        :return: yield value from input list, input file or input Graph object
        """
        if self.input_graph is None:
            if self.input is None:
                with open(self.input_file, "r") as file:
                    for line in file.readlines():
                        yield json.loads(line)
            else:
                for value in self.input:
                    yield value
        else:
            for value in self.input_graph.res:
                yield value


class Map(Node):
    """ Node class which provides Map operation. """

    def __init__(self, operation, input=None, output=None, name=None):
        """
        :param operation: map generator to apply for input values.
        :param input: input Node object.
        :param output: output Node object.
        :param name: name of this Node object.
        """
        super().__init__(input=input, output=output, name=name)
        self.operation = operation

    def run(self):
        """
        Apply map operation to each value from output of input Node object and
        then yield it forward to Graph computations.
        """
        for value in self.input.run():
            yield from self.operation(value)


class Sort(Node):
    """ Node class which provides Sort operation. """

    def __init__(self, by, input=None, output=None, name=None):
        """
        :param by: string or list of keys.
        :param input: input Node object.
        :param output: output Node object.
        :param name: name of this Node.
        """
        super().__init__(input=input, output=output, name=name)
        if isinstance(by, str):
            self.by = [by]
        elif isinstance(by, list):
            self.by = by
        else:
            raise ValueError("Unknown type for _by_ value\n")

    def run(self):
        """
        Sort a result of input Node object work.
        :return: yield sorted values from previous node
        """
        result = list(self.input.run())
        result = sorted(result, key=itemgetter(*self.by))
        for value in result:
            yield value


class Join(Node):
    """ Node class which provides Join operation. """

    def __init__(self, on, key, strategy, input=None, output=None, name=None):
        """
        :param on: Graph object which is joined.
        Graph object is always LEFT for joining operation.

        :param key: string or list of string. Keys for join operation
        :param strategy: string. SQL strategy of join. Supports inner, outer,
        left and right strategies.

        :param input: input Node object.
        :param output: output Node object.
        :param name: name of this Node.
        """
        super().__init__(input=input, output=output, name=name)
        self.graph = on
        self.strategy = strategy

        if isinstance(key, str):
            self.key = [key]
        else:
            if len(key) > 0:
                self.key = key
            else:
                self.key = []
                if self.strategy == "outer":
                    self.strategy = "cross"

    def run(self):
        """
        Join two lists of dicts and then yield values
        from the result of joining.

        Joining function works through Sort and Reduce operations.

        Work with N log N asymptotics.
        Distribute work of joining to different strategies function of joining.

        If dicts in left table and right table have common names of columns
        then add "left_" and "right_" prefix to these names in result columns
        (if common columns are not in self.key).
        """

        # self.res is a result of input Graph object. This value could not
        # be calculated on initialization step.
        # self.output is a result of input Node work.

        self.res = self.graph.res
        self.output = list(self.input.run())

        common_columns = (set(self.res[0].keys()) &
                          set(self.output[0].keys())) - set(self.key)

        for column in common_columns:
            for left in self.res:
                left["left_" + column] = left.pop(column)

            for right in self.output:
                right["right_" + column] = right.pop(column)

        self.left_keys = list(self.res[0].keys())
        self.right_keys = list(self.output[0].keys())

        if self.strategy == "inner":
            yield from self._inner_run()

        elif self.strategy == "left":
            yield from self._left_run()

        elif self.strategy == "right":
            yield from self._right_run()

        elif self.strategy == "outer":
            yield from self._outer_run()

        elif self.strategy == "cross":
            yield from self._cross_run()

    def _inner_reducer(self, records):
        """
        :param records: records with similar keys.
        :return: yield values from INNER join table.
        """
        if len(records) > 1:
            first_elem_keys = records[0].keys()
            second_elem_keys = records[1].keys()
            flag = False

            for first_key, second_key in zip(first_elem_keys,
                                             second_elem_keys):
                if first_key != second_key:
                    flag = True
                    break

            if flag:
                for value in records[1:]:
                    value.update(records[0])
                    new_value = {}
                    for key in sorted(value):
                        new_value[key] = value[key]
                    yield new_value

            else:
                for value in records[:-1]:
                    value.update(records[-1])
                    new_value = {}
                    for key in sorted(value):
                        new_value[key] = value[key]
                    yield new_value

    def _inner_run(self):
        """
        1. Sort input tables.
        2. Create mini Graph object with only one Reduce Node
        object with _inner_reducer operation.
        3. Run this mini Graph object.
        :return: yield values from INNER joined table.
        """
        summary = sorted(self.res + self.output, key=itemgetter(*self.key))
        current_input = Input(input=summary)
        reducer = Reduce(self._inner_reducer, key=self.key)(current_input)
        current_graph = Graph(input_node=current_input, output_node=reducer)
        yield from current_graph.run()

    def _left_reducer(self, records):
        """
        :param records: records with similar keys.
        :return: yield values from LEFT joined table.
        """
        if len(records) > 1:
            yield from self._inner_reducer(records)

        if len(records) == 1:
            for k1, k2 in zip(records[0].keys(), self.left_keys):
                if k1 != k2:
                    break
            else:
                records[0].update({key: None for key in self.right_keys
                                  if key not in self.left_keys})

                new_value = {}
                for key in sorted(records[0]):
                    new_value[key] = records[0][key]
                yield new_value

    def _left_run(self):
        """
        1. Sort input tables.
        2. Create mini Graph object with only one Reduce Node
        object with _inner_reducer operation.
        3. Run this mini Graph object.
        :return: yield values from LEFT joined table.
        """
        summary = sorted(self.res + self.output, key=itemgetter(*self.key))
        current_input = Input(summary)
        reducer = Reduce(self._left_reducer, key=self.key)(current_input)
        current_graph = Graph(input_node=current_input, output_node=reducer)
        yield from current_graph.run()

    def _right_reducer(self, records):
        """ Similar to _left_reducer. """
        if len(records) > 1:
            yield from self._inner_reducer(records)

        if len(records) == 1:
            for k1, k2 in zip(records[0].keys(), self.right_keys):
                if k1 != k2:
                    break
            else:
                records[0].update({key: None for key in self.left_keys
                                   if key not in self.right_keys})

                new_value = {}
                for key in sorted(records[0]):
                    new_value[key] = records[0][key]
                yield new_value

    def _right_run(self):
        """ Similar to _left_run. """
        summary = sorted(self.output + self.res, key=itemgetter(*self.key))
        current_input = Input(summary)
        reducer = Reduce(self._right_reducer, key=self.key)(current_input)
        current_graph = Graph(input_node=current_input, output_node=reducer)
        yield from current_graph.run()

    def _outer_reducer(self, records):
        """ Similar to _left_reducer. """
        if len(records) > 1:
            yield from self._inner_reducer(records)

        if len(records) == 1:
            value = {key: value for key, value in records[0].items()}

            for k1, k2 in zip(records[0].keys(), self.right_keys):

                if k1 != k2:
                    value.update({key: None for key in self.right_keys
                                       if key not in self.left_keys})
                    break

            else:
                value.update({key: None for key in self.left_keys
                                   if key not in self.right_keys})

            new_value = {}
            for key in sorted(value):
                new_value[key] = value[key]
            yield new_value

    def _outer_run(self):
        """ Similar to _left_run. """
        summary = sorted(self.output + self.res, key=itemgetter(*self.key))
        current_input = Input(summary)
        reducer = Reduce(self._outer_reducer, key=self.key)(current_input)
        current_graph = Graph(input_node=current_input, output_node=reducer)
        yield from current_graph.run()

    def _cross_run(self):
        """
        Run when strategy == 'outer' and key is None.
        :return: yield value from CROSS joined table.
        """
        for first_dict in self.res:
            for second_dict in self.output:
                first_dict.update(second_dict)
                new_value = {}
                for key in sorted(first_dict):
                    new_value[key] = first_dict[key]
                yield new_value


class Fold(Node):
    """ Node class which provides Fold operation. """

    def __init__(self, function, start_state, input=None,
                 output=None, name=None):
        """
        :param function: function to apply in fold operation.
        :param start_state: start state for fold operation.
        :param input: input Node object.
        :param output: output Node object.
        :param name: name of this Node object.
        """
        super().__init__(input=input, output=output, name=name)
        self.fold_function = function
        self.state = start_state

    def run(self):
        """ Apply fold operation to result of input Node object. """
        for value in self.input.run():
            self.state = self.fold_function(self.state, value)

        yield self.state


class Reduce(Node):
    """ Node class which provides Reduce operation. """

    def __init__(self, operation, key=None, input=None,
                 output=None, name=None):
        """
        :param operation: generator with reduce operation.
        :param key: string or list of string with keys.
        :param input: input Node object.
        :param output: output Node object.
        :param name: name of this Node.
        """
        super().__init__(input=input, output=output, name=name)
        self.operation = operation

        if isinstance(key, str):
            self.key = [key]
        else:
            self.key = key

    def run(self):
        """
        Make blocks with equal keys from result of input Node object,
        pass these blocks to reduce generator and yield value from it
        :return:
        """
        if self.key is None:
            yield from self.operation(list(self.input.run()))
        else:

            previous_key = None
            stack = []

            for current_value in self.input.run():

                current_key = {key: current_value[key] for key in self.key}

                if previous_key is None:
                    stack.append(current_value)
                else:
                    if current_key == previous_key:
                        stack.append(current_value)
                    else:
                        yield from self.operation(stack)
                        stack = [current_value]

                previous_key = current_key

            if len(stack) > 0:
                yield from self.operation(stack)


class Graph(object):
    """ Graph class for construct and run computing graphs. """

    def __init__(self, input_node, output_node, name=None):
        """
        :param input_node: input Node object. Type of input_node must
        be strictly Input Node.

        :param output_node: output Node object.
        :param name: name of this graph.
        """
        self.input_node = input_node
        self.output_node = output_node
        self.name = name

        # self._dependencies is a list of Graph objects with graphs
        # which necessary should be already computed.
        self._dependencies = []

        # self._used is flag for Depth-first search.
        self._used = False

        # self.res is a result of computation of this graph.
        self.res = None

        self.nodes = self._create_node_list()
        for node in self.nodes:
            if isinstance(node, Join):
                if node.graph not in self._dependencies:
                    self._dependencies.append(node.graph)

            if isinstance(node, Input):
                if node.input_graph is not None:
                    if node.input_graph not in self._dependencies:

                        self._dependencies.append(node.input_graph)

        self._topological_sort()

    def _create_node_list(self):
        """
        Create list of nodes in Graph through moving backward from
        self.output_node to self.input_node
        :return: list of nodes in Graph
        """
        result = []
        current_node = self.output_node
        while current_node != self.input_node:
            result.append(current_node)
            if current_node.input is None:
                raise RuntimeError("Input is None in node {}".format(current_node))

            current_node = current_node.input
        result.append(self.input_node)
        return result[::-1]

    def _topological_sort(self):
        """ Make graph topological sort for oprimize calculations. """

        # self.order is a list with right computing order
        self.order = []
        for graph in self._dependencies:
            if not graph._used:
                self._depth_first_search(graph)

        for graph in self._dependencies:
            graph._used = False

    def _depth_first_search(self, graph):
        """ Make Depth-first search on graph dependencies. """
        graph._used = True
        for current_graph in graph._dependencies:
            if not current_graph._used:
                self._depth_first_search(current_graph)

        self.order.append(graph)

    def run(self, inputs=None, input_file=None,
            output_file=None, verbose=False):
        """
        :param inputs: dictionary {graph: path_to_input_file}.
        :param input_file: path to input file (only if inputs is None).
        :param output_file: file object in which result will be written.
        :param verbose: verbose flag.
        :return: list with dicts which is a result of computing.
        """
        if verbose:
            print("Computing in {}\n".format(self.name))
        res = []

        if inputs is not None:
            for graph, file in inputs.items():
                for graph_depend in self.order:
                    if graph_depend == graph:
                        graph_depend.input_node.input_file = file
                        break

        elif input_file is not None:
            self.input_node.input_file = input_file

        for graph in self.order:
            if graph.res is None:
                graph.res = graph.run(verbose=verbose)

        for i in self.nodes[-1].run():
            res.append(i)

        if output_file is not None:
            for line in res:
                output_file.write(json.dumps(line) + "\n")
        else:
            return res
