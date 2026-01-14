from functools import wraps

def error_context(msg, fmt_args:list[int]|None = None, fmt_kwargs:list[str]|None=None):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                fmt = []
                if fmt_args is not None:
                    fmt += [args[i] for i in fmt_args] 
                if fmt_kwargs is not None:
                    fmt += [kwargs[k] for k in fmt_kwargs] 
                raise MetropyError(msg.format(*fmt)) from e
        return wrapper
    return decorate


class MetropyError(Exception):
    def __init__(self, *args):
        super().__init__(*args)

    def __str__(self):
        """Format the error message to include the reason in a structured way."""
        messages = [super().__str__()]
        cause = self.__cause__

        if isinstance(cause, MetropyError):
            messages.append("\nCaused by:")

        i = 0
        while isinstance(cause, MetropyError):
            messages.append(f"\t{i}: {cause.args[0]}")
            cause = cause.__cause__
            i += 1

        if cause:
            messages.append(f"\t{i}: {cause.__class__.__name__}: {cause}")

        return "\n".join(messages)
