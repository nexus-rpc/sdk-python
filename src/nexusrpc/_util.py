from typing import Any

# TODO(preview): backport inspect.get_annotations
# See
# https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older
# https://github.com/shawwn/get-annotations/blob/main/get_annotations/__init__.py
try:
    from inspect import get_annotations as _get_annotations

    def get_annotations(obj: Any) -> dict[str, Any]:
        return _get_annotations(obj)
except ImportError:

    def get_annotations(obj: Any) -> dict[str, Any]:
        return getattr(obj, "__annotations__", {})
