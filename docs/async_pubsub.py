from __future__ import annotations

import asyncio
import os
import sys

import redis.asyncio as redis

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
crawler_dir = os.path.join(parent_dir, "simple_crawler")

sys.path.append(parent_dir)
sys.path.append(crawler_dir)


STOPWORD = "STOP"
import logging

logger = logging.getLogger(__name__)

# class MyListener(BaseListener):
#     def __init__(self, pubsub, *args):
#         super().__init__(pubsub, *args)
#         self.running= True
#         logger.info(f"Subscribed to {pubsub}. Waiting for messages...")


#     def handle_message(self):
#         while self.running:
#             for message in self.pubsub.listen():
#                 logger.debug(f"Received message: {message}")
#                 if message["type"] == "message":
#                     if message["data"] == b"exit":
#                         self.running = False
#                         break
#                     else:
#                         url_data = json.loads(message.get("data", ""))
#                         self.store_url(url_data)
#         self.flush_urls()
#         exit_event.set()

#     def handle_message(self):
#         while True:
#             message = await channel.get_message(ignore_subscribe_messages=True)
#             if message is not None:
#                 print(f"(Reader) Message Received: {message}")
#                 if message["data"].decode() == STOPWORD:
#                     print("(Reader) STOP")
# break
#


async def reader(channel: redis.client.PubSub):
    while True:
        message = await channel.get_message(ignore_subscribe_messages=True)
        if message is not None:
            print(f"(Reader) Message Received: {message}")
            await asyncio.sleep(2)
            print(f"(Reader) Done sleeping {message}")
            if message["data"].decode() == STOPWORD:
                print("(Reader) STOP")
                break


async def do_something_else():
    for i in range(3):
        await asyncio.sleep(1)
        print("(Reader) Doing something else")
        await asyncio.sleep(2)


def add_listener(self, pubsub, listener_cls, args=()):
    handler = listener_cls(pubsub, *args)
    handler.start()
    self.listeners.append(handler)


r = redis.Redis(host="localhost", port=7777)


async def main():
    async with r.pubsub() as pubsub:
        await pubsub.subscribe("channel:1", "channel:2")

        future = asyncio.create_task(reader(pubsub))

        await do_something_else()
        await r.publish("channel:1", "Hello")
        await r.publish("channel:2", "World")
        await r.publish("channel:1", STOPWORD)

        await future
    #### RESULTS ####
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'Hello'}
    # (Reader) Doing something else
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'Hello'}
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:2', 'data': b'World'}
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:2', 'data': b'World'}
    # (Reader) Message Received: {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'STOP'}
    # (Reader) Doing something else
    # (Reader) Done sleeping {'type': 'message', 'pattern': None, 'channel': b'channel:1', 'data': b'STOP'}
    # (Reader) STOP
    # (Reader) Doing something else
    # (venv) ╭─penguaman at penguaputer


if __name__ == "__main__":
    asyncio.run(main())

    # add_listener(r, reader, BaseListener)
    # asyncio.run(main())
