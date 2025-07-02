# from flask import Flask, jsonify
# # from main import crawl
# app = Flask(__name__)


from __future__ import annotations

from flask import Flask, jsonify
from main import crawl

app = Flask(__name__)


@app.route("/")
def hello_world():
    return jsonify({"message": "Hello, World!"})


@app.route("/crawl/<url>/<max_depth>/<retries>/<check_every>/<rq_crawl>")
def crawl_endpoint(url: str, max_depth: int, retries: int, check_every: int):
    links = crawl(url, max_depth, retries, check_every, rq_crawl=True)
    return links


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)
