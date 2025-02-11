from dags.output import aggregated_output, dict_output, list_output, single_output


def test_single_output_decorator():
    @single_output
    def f():
        return (1,)

    assert f() == 1


def test_dict_output_decorator():
    @dict_output(keys=["a", "b"])
    def f():
        return (1, 2)

    assert f() == {"a": 1, "b": 2}


def test_list_output_decorator():
    @list_output
    def f():
        return (1, 2)

    assert f() == [1, 2]


def test_aggregated_output_decorator():
    @aggregated_output(aggregator=lambda x, y: x + y)
    def f():
        return (1, 2)

    assert f() == 3


def test_single_output_direct_call():
    def f():
        return (1,)

    g = single_output(f)

    assert g() == 1


def test_dict_output_direct_call():
    def f():
        return (1, 2)

    g = dict_output(f, keys=["a", "b"])

    assert g() == {"a": 1, "b": 2}


def test_list_output_direct_call():
    def f():
        return (1, 2)

    g = list_output(f)

    assert g() == [1, 2]


def test_aggregated_output_direct_call():
    def f():
        return (1, 2)

    g = aggregated_output(f, aggregator=lambda x, y: x + y)
    assert g() == 3
