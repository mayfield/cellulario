from cellulario import iocell
import asyncio
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

iocell.DEBUG = True

