# from flask import Flask, jsonify
# # from main import crawl
# app = Flask(__name__)


from __future__ import annotations

import redis
from main import crawl
from rq import Queue, Worker

# @app.route('/')
# def hello_world():
#     return jsonify({"message": "Hello, World!"})

# # @app.route('/crawl/<url>/<max_depth>/<retries>/<check_every>')
# # def crawl_endpoint(url: str, max_depth: int, retries: int, check_every: int):
# #     links = crawl(url, max_depth, retries, check_every)
# #     return links


# def test(message: str) -> str:
#     print(message)
#     return message


# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5000)


rdb = redis.Redis(host="localhost", port=7777)


def rq_crawl(url: str, max_pages: int, retries: int, check_every: int):
    rdb.delete("to_visit")
    rdb.delete("download_requests")
    rdb.delete("parse_requests")

    queue = Queue(connection=rdb, name="test")
    queue.enqueue(crawl, url, max_pages, retries, check_every)

    worker = Worker(queue, connection=rdb)
    worker.work(burst=True)

    print("hi")
