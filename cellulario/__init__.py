
import functools
import platform


def monkey_patch_issue_25593():
    """ Workaround for http://bugs.python.org/issue25593 """
    from asyncio.selector_events import BaseSelectorEventLoop
    save = BaseSelectorEventLoop._sock_connect_cb

    @functools.wraps(save)
    def patched(instance, fut, sock, address):
        if not fut.done():
            save(instance, fut, sock, address)
    BaseSelectorEventLoop._sock_connect_cb = patched

if platform.python_version() <= '3.5.0':
    monkey_patch_issue_25593()

from .iocell import *
from .coordination import *
from .tier import *
