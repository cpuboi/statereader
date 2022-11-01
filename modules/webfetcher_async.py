#/usr/bin/env python3

"""
Asynchronous fetcher of websites.
This is just a test function
"""

import asyncio
import aiohttp

def fetch_and_return(__url):
    if not isinstance(__url, str):
        __url=str(__url.decode()).strip()
    __url = __url.strip()
    async def run_async(__url):
        async with aiohttp.ClientSession() as session:
            __response = await session.get(__url)
            __html = await __response.text()
            print(__html)
            return __response, __html
    __response = asyncio.run(run_async(__url))
    return __response

