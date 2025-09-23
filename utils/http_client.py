import ssl
from typing import Optional

import aiohttp
import certifi


def create_aiohttp_session() -> aiohttp.ClientSession:
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    return aiohttp.ClientSession(connector=connector)



