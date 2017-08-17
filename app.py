import logging
import asyncio
import os
import time
from aiohttp import web
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
HOST = "127.0.0.1"
PORT = "9000"


async def init(loop):
    app = web.Application(loop)
    app.router.add_route("GET", "/", index)
    server = await loop.create_server(app.make_handler(), HOST, PORT)
    logging.info("Server started at http://{}:{}".format(HOST, PORT))
    return server


def index(request):
    resp = "<h1> Hello World </h1>"
    return web.Response(body=resp.encode())


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()

if __name__ == "__main":
    main()