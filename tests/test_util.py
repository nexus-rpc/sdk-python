from functools import partial

from nexusrpc._handler._util import is_async_callable


def test_async_def_is_async_callable():
    async def f(a: int, b: int) -> None:
        pass

    assert is_async_callable(f)
    assert is_async_callable(partial(f, a=1))


def test_async_callable_instance_is_async_callable():
    class f_cls:
        async def __call__(self, a: int, b: int) -> None:
            pass

    f = f_cls()
    g = partial(f, a=1)

    assert is_async_callable(f)
    assert is_async_callable(g)
