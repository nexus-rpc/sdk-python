from functools import partial

from nexusrpc._util import is_async_callable, is_callable


def test_def():
    def f(a: int, b: int) -> None:
        pass

    g = partial(f, a=1)
    assert is_callable(f)
    assert is_callable(g)
    assert not is_async_callable(f)
    assert not is_async_callable(g)


def test_callable_instance():
    class f_cls:
        def __call__(self, a: int, b: int) -> None:
            pass

    f = f_cls()
    g = partial(f, a=1)

    assert is_callable(f)
    assert is_callable(g)
    assert not is_async_callable(f)
    assert not is_async_callable(g)


def test_async_def():
    async def f(a: int, b: int) -> None:
        pass

    g = partial(f, a=1)
    assert is_callable(f)
    assert is_callable(g)
    assert is_async_callable(f)
    assert is_async_callable(g)


def test_async_callable_instance():
    class f_cls:
        async def __call__(self, a: int, b: int) -> None:
            pass

    f = f_cls()
    g = partial(f, a=1)

    assert is_callable(f)
    assert is_callable(g)
    assert is_async_callable(f)
    assert is_async_callable(g)


def test_partial():
    def f(a: int, b: int) -> None:
        pass

    g = partial(f, a=1)
