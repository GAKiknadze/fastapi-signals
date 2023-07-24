import re
import asyncio
from inspect import iscoroutinefunction
from fastapi_signals.exceptions import AlreadyExists


_background_tasks = set()

_signal_handlers = dict()


def register(pattern: str):
    if pattern in _signal_handlers.keys():
        raise AlreadyExists

    def wrapper(func):
        _signal_handlers.update({pattern: func})
    return wrapper


def dispatch(signal: str, context: dict[str, any] = dict()):
    tasks = []
    
    async def task():
        await asyncio.gather(*tasks)
    
    for pattern, func in _signal_handlers.items():
        result = re.match(pattern, signal)
        
        if result is None:
            continue
        
        groups = result.groups()
        
        if iscoroutinefunction(func):
            coro = func(*groups, **context)
        else:
            coro = asyncio.to_thread(func, *groups, **context)
        
        tasks.append(coro)
    
    bg_task = asyncio.create_task(task())
    _background_tasks.add(bg_task)
    bg_task.add_done_callback(_background_tasks.discard)
