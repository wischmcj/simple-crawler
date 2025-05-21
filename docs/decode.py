from __future__ import annotations

import asyncio
import json

from redis import Redis as rRedis
from redis.asyncio import Redis

url = "http://example.com/public/"
content = "<html>Test</html>"
mapping = {
    "seed_url": "http://example.com",
    "content": content,
    "req_status": 200,
    "crawl_status": "downloaded",
    "run_id": "run_0",
    "max_pages": 100,
}

r = Redis(host="localhost", port=7777)


async def deserialize(in_obj):
    """
    Recursively decode a redis hgetall response.
    """
    if isinstance(in_obj, list):
        ret = list()
        for item in in_obj:
            await ret.append(deserialize(item))
        return ret
    elif isinstance(in_obj, dict):
        ret = dict()
        for key in in_obj:
            ret[key.decode()] = await deserialize(in_obj[key])
        return ret
    elif isinstance(in_obj, bytes):
        # Assume string encoded w/ utf-8
        return in_obj.decode("utf-8")
    else:
        raise Exception(f"type not handled: {type(in_obj)}")


async def add_linked_urls(in_obj):
    linked_urls = [
        " https://github.com/hadialqattan/pycln",
        "https://github.com/asottile/pyupgrade",
        "https://github.com/codespell-project/codespell",
    ]
    encoded = json.dumps(in_obj["linked_urls"])
    await r.hset(url, mapping={"linked_urls": encoded})
    for key in in_obj:
        await add_linked_urls(in_obj[key])


conn = rRedis(host="localhost", port=7777)


async def main():
    await r.hset(url, mapping=mapping)
    response = await r.hgetall(url)
    res = await deserialize(response)
    return res


if __name__ == "__main__":
    test = asyncio.run(main())
    print(test)
    breakpoint()
