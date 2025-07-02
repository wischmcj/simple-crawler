
from rq import Connection, Worker
import redis


def run_worker():
    redis_connection = redis.Redis(host="localhost", port=7777)
    with Connection(redis_connection):
        worker = Worker(["dowmload"])
        worker.work()

if __name__ == "__main__":
    run_worker()
