
from abc import abstractmethod
import logging
import threading
import time

import redis
import rq
from rq import Worker, Queue
from rq.command import send_shutdown_command

from concurrent.futures import ThreadPoolExecutor
from rq.registry import FailedJobRegistry, StartedJobRegistry, FinishedJobRegistry

from config.configuration import get_logger, MAX_WORKERS
from helper_classes import BaseListener
from utils import add

logger = get_logger('scheduler')


class BaseWorkClass(Worker):
    def __init__(
            self,
            connection,
            queues,
            worker_class='base',
            host="localhost",
            port=7777,
    ):
        super().__init__(connection=connection, queues=queues)
        self.host = host
        self.port = port
        self.work_class = worker_class # I could probably use actual class here ...
        self.continue_on_failure = False

    # @abstractmethod
    def on_success(job, connection, result):
        logger.info(f"download {job.args[0]} succeeded")
        connection.sadd('success', job.id) # primarily for testing
        pass

    def on_failure(job, connection, type, value, traceback):
        """Callback for when a job succeeds"""
        url = job.meta["url"]
        logger.info(f"{job.meta['func_name']} for {url} failed")
        connection.sadd('failed', job.id) # primarily for testing

    def work(self, burst=True):
        super().work(burst=burst)


class QueueHandler:
    def __init__(self, pubsub, queue, workforce, worker_class=BaseWorkClass):
        """Initialize with a Redis pubsub object"""
        self.pubsub = pubsub
        self.queue = queue
        self.running = True
        self.workforce = workforce
        self.worker_class = worker_class

    def start(self):
        """Start the message handling loop in a separate thread"""
        self.thread = threading.Thread(target=self.handle_messages)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        """Stop the message handling loop"""
        self.running = False
        self.thread.join()

    def enqueue_job(self, job):
        """Enqueue a job"""
        logger.info(f"Enqueuing job")
        self.queue.enqueue(add, args=(1, 2, 2))

    def start_worker(self):
        """Start a new worker"""
        logger.info(f"Starting worker {self.worker_class.name}")
        worker = self.worker_class(connection=self.redis_conn, queues=[self.queue.name])
        worker.work(burst=True)

    def handle_messages(self):
        """Main message handling loop"""
        while self.running:
            for message in self.pubsub.listen():
                if message["type"] == "message":
                    curr_url = message["data"].decode("utf-8")
                    logger.info(f"Received message: {curr_url}")
                    job = self.queue.enqueue(add, args=(1, 4))
                    logger.info(f'Handler Started {job.id}')
                    self.enqueue_job()
                    self.start_worker()
                    # self.workforce.add_job(job)
                    # self.workforce._start_worker()


def start_worker(queues, redis_conn, worker_class=BaseWorkClass):
    worker = worker_class(connection=redis_conn, queues=queues)
    logger.info(f"TEST Starting worker {worker.name}")
    worker.work(burst=True)
    logger.info(f"TEST Started worker")

class Workforce:
    def __init__(self, 
                 redis_conn=None, 
                 queue=None, 
                 worker_class=BaseWorkClass,
                 debug=False):
        self.redis_conn = redis_conn
        self.queue = queue
        self.num_workers = MAX_WORKERS
        self.worker_class = worker_class
        self.debug = debug
        self.finished_jobs = []
        self.listeners = []
        channel = "mychannel"
        self.add_listener(channel)
        # self._start_worker()
        self.request_jobs()
        self.wait_for_jobs_to_finish()

    # def _start_worker(self):
    #     """
    #     Start rq workers for each queue.
    #     Workers can be accessed via our redis connection object, so
    #     we don't save pointers to the workers in the manager.
    #     """
    #     start_worker([self.queue.name], self.redis_conn, self.worker_class)
        # executor = ThreadPoolExecutor(max_workers=2)
        # for i in range(self.num_workers):
            # print(f"Started worker {i} for queue: {self.queue}")
        # executor.submit(start_worker, [self.queue.name], self.redis_conn, self.worker_class)

    def _stop_workers(self):
        logger.info("Stopping workers gracefully")
        worker_stats = {}
        workers = Worker.all(self.redis_conn)
        for worker in workers:
            worker_stats[worker.name] = {
                "successful_jobs": worker.successful_job_count,
                "failed_jobs": worker.failed_job_count,
                "total_working_time": worker.total_working_time,
            }
            send_shutdown_command(self.redis_conn, worker.name)
        workers = Worker.all(self.redis_conn)
        print(f"{workers=}")
        # Wait for workers to close
        time.sleep(1)

    def wait_for_jobs_to_finish(self):
        time.sleep(5)
        finished_registry = FinishedJobRegistry(queue=self.queue)
        started_registry = StartedJobRegistry(queue=self.queue)
        failed_registry = FailedJobRegistry(queue=self.queue)
        finished_job_ids = finished_registry.get_job_ids()
        started_job_ids = started_registry.get_job_ids()
        failed_job_ids = failed_registry.get_job_ids()
        queued_job_ids = self.queue.jobs
        while True:
            finished_registry = FinishedJobRegistry(queue=self.queue)
            started_registry = StartedJobRegistry(queue=self.queue)
            failed_registry = FailedJobRegistry(queue=self.queue)
            finished_job_ids = finished_registry.get_job_ids()
            started_job_ids = started_registry.get_job_ids()
            failed_job_ids = failed_registry.get_job_ids()
            queued_job_ids = self.queue.jobs
            print(f"{finished_job_ids=}")
            print(f"{started_job_ids=}")
            print(f"{failed_job_ids=}")
            print(f"{queued_job_ids=}")
            workers = Worker.all(self.redis_conn)
            if len(queued_job_ids) >0:
                print(f"{workers=}")
                print([w.get_state() for w in workers])
                # send_shutdown_command(workers[0])
                # idle_workers = [w for w in workers if w.state == 'idle']
                # if len(workers) == 0:
                    # self._start_worker()
                # else:
                    # pass
            else:
                break
            time.sleep(5)

    ## Specify shutdown behavior
    def shutdown(self):
        self.wait_for_jobs_to_finish()
        try:
            self._stop_workers()
        except rq.worker.StopRequested:
            logger.info(f"Workers stopped")

    def request_jobs(self):
        logger.info(f"Requesting jobs")
        self.redis_conn.publish('mychannel', 'test')
        time.sleep(1)
        self.redis_conn.publish('mychannel', 'test')
        time.sleep(1)

    def add_listener(self, channel):
        pubsub = self.redis_conn.pubsub()
        pubsub.subscribe(channel)
        print(f"Subscribed to {channel}. Waiting for messages...")
        handler = QueueHandler(pubsub=pubsub, queue=self.queue, workforce=self, worker_class=BaseWorkClass)
        handler.start()
        self.listeners.append(handler)
    
    def add_job(self, job):
        self.finished_jobs.append(job)
        print(f"Finished job {job.id}")


    # def manage(self):
    #     """
    #     This represents the crawling of a single page.
    #     1. Download the content, if possible, cache to redis
    #     2. Extract urls from the page, return them
    #     3. Enqueue the urls for parsing
    #     """
    #     registry = FinishedJobRegistry(queue=self.queue)
    #     finished_job_ids = registry.get_job_ids()
    #     while True:
    #         time.sleep(5)
    #         print(f"{finished_job_ids=}")
    #         jobs_finished = all([jid in finished_job_ids for jid in self.jobs])
    #         if jobs_finished:
    #             logger.info(f"All jobs finished")
    #             self.shutdown()
    #             break
            # queue_size = len(self.queue)
            # workers = Worker.all(self.redis_conn)
            # active_workers = len(workers)
            # idle_workers = len([w for w in workers if w.state == 'idle'])
            # if queue_size == 0 and idle_workers > 1:
            #     logger.info(f"Queue empty, removing {idle_workers-1} idle workers")
            #     idle_count = 0
            #     idle_loops+=1
            #     for worker in workers:
            #         if worker.state == 'idle' and idle_count > 0:
            #             send_shutdown_command(self.redis_conn, worker.name)

    # def manage_workers(self):
    #     """Monitor queue size and worker count, adjusting workers as needed"""
    #     idle_loops = 0
    #     while True:
    #         try:
    #             # Get current queue and worker stats
    #             queue_size = len(self.queue)
    #             workers = Worker.all(self.redis_conn)
    #             active_workers = len(workers)
    #             idle_workers = len([w for w in workers if w.state == 'idle'])

    #             # Remove idle workers if queue is empty
    #             if queue_size == 0 and idle_workers > 1:
    #                 logger.info(f"Queue empty, removing {idle_workers-1} idle workers")
    #                 idle_count = 0
    #                 idle_loops+=1
    #                 for worker in workers:
    #                     if worker.state == 'idle' and idle_count > 0:
    #                         send_shutdown_command(self.redis_conn, worker.name)
    #                         idle_count += 1

    #             # Add workers if queue growing and below max
    #             elif queue_size > active_workers and active_workers < self.num_workers:
    #                 logger.info(f"Queue larger than num workers, adding {queue_size - active_workers} workers")
    #                 workers_to_add = min(
    #                     queue_size - active_workers,
    #                     self.num_workers - active_workers
    #                 )
    #                 logger.debug(f"Adding {workers_to_add} workers")
    #                 executor = ThreadPoolExecutor(max_workers=workers_to_add)
    #                 for i in range(workers_to_add):
    #                     executor.submit(
    #                         start_worker,
    #                         self.queue.name,
    #                         self.redis_conn,
    #                         self.worker_class
    #                     )
    #             if idle_loops > 2:
    #                 logger.info("No workers for 2 loops, shutting down")
    #                 self.shutdown()
    #                 break
    #             time.sleep(5)  # Check every 5 seconds

    #         except Exception as e:
    #             logger.error(f"Error managing workers: {e}")
    #             break