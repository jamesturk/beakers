from typing import Callable


def callable_name(c: Callable) -> str:
    if hasattr(c, "__name__"):
        return "λ" if c.__name__ == "<lambda>" else c.__name__
    else:
        return repr(c)
