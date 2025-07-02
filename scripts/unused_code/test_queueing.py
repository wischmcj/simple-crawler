import pytest
import redis
import time
import rq
from rq import Queue, Worker, Callback
from rq.registry import FailedJobRegistry, StartedJobRegistry, FinishedJobRegistry
from rq.command import send_shutdown_command

from queueing import Workforce, BaseWorkClass
from utils import add

from config.configuration import get_logger, MAX_WORKERS

logger = get_logger('scheduler')

@pytest.fixture
def redis_conn():
    # Use a different DB number for tests to avoid conflicts
    r = redis.Redis(host="localhost", port=7777)
    return r

@pytest.fixture
def test_queue(redis_conn):
    q = Queue(connection=redis_conn, name="test_queue")
    return q

@pytest.fixture
def failed_jr(test_queue):
    registry = FailedJobRegistry(queue=test_queue)
    return registry


@pytest.fixture
def finished_jr(test_queue):
    registry = FinishedJobRegistry(queue=test_queue)
    return registry


@pytest.fixture
def started_jr(test_queue):
    registry = StartedJobRegistry(queue=test_queue)
    return registry

class NonBaseWorkClass(BaseWorkClass):
    def on_success(job, connection, result):
        logger.info(f"download {job.args[0]} succeeded")
        connection.sadd('NonBaseWorkClass_success', job.id)

# def test_base_worker_success_sync(redis_conn, test_queue, finished_jr):
#     """Testing callbacks using the custom BaseWorkClass"""
#     # Enqueue a job
#     job = test_queue.enqueue(add, args=(1, 2), on_success=Callback(BaseWorkClass.on_success))
    
#     # Start worker
#     worker = BaseWorkClass(connection=redis_conn, queues=["test_queue"])
#     worker.work(burst=True)
    
#     # Check job was processed
#     assert  job.id in  finished_jr.get_job_ids()
#     # check that the on success callback was called
#     assert redis_conn.sismember('success', job.id)
#     test_queue.empty()


# def test_non_base_worker_success(redis_conn, test_queue, finished_jr):
#     """Testing callbacks using subclasses of the BaseWorkClass"""
#     # Enqueue a job
#     job = test_queue.enqueue(add, args=(1, 2), on_success=Callback(NonBaseWorkClass.on_success))
    
#     # Start worker
#     worker = NonBaseWorkClass(connection=redis_conn, queues=["test_queue"])
#     worker.work(burst=True)
    
#     # Check job was processed
#     assert  job.id in  finished_jr.get_job_ids()
#     # check that the on success callback was called
#     assert redis_conn.sismember('NonBaseWorkClass_success', job.id)
#     test_queue.empty()


# def test_workforce_initialization_teardown(redis_conn, test_queue, finished_jr):
#     """Testing the workforce initialization and teardown"""
#     # redis_conn.flushdb()
#     workforce = Workforce(redis_conn=redis_conn, queue=test_queue, worker_class=BaseWorkClass)
#     job = test_queue.enqueue(add, args=(1, 2))
    
#     # Give workers time to process the job
#     time.sleep(2)
#     # Check workers were created
#     workers = Worker.all(redis_conn)
#     assert  job.id in  finished_jr.get_job_ids()

#     # Shutdown workers
#     workforce.shutdown()

#     # Wait for rq to update worker status
#     time.sleep(2)
#     # Check workers were stopped
#     workers = Worker.all(redis_conn)
#     assert len(workers) == 0

# def test_workforce_job_processing(redis_conn, test_queue, finished_jr):
#     workforce = Workforce(redis_conn=redis_conn, queue=test_queue, worker_class=BaseWorkClass,debug=False)

#     job = test_queue.enqueue(add, args=(1, 2))
#     workforce.add_job(job)
#     workforce.shutdown()

#     queued_job_ids = test_queue.jobs
#     assert len(queued_job_ids) == 0

def test_workforce_shutdown(redis_conn, test_queue):    
    # job = test_queue.enqueue(add, args=(1, 2))
    workforce = Workforce(redis_conn=redis_conn, queue=test_queue, debug=True)
    # workforce.wait_for_jobs_to_finish()
    workers = Worker.all(redis_conn)
    assert len(workers) == 0

# def test_workforce_shutdown(redis_conn, test_queue):
#     workforce = Workforce(redis_conn=redis_conn, queue=test_queue)
    
#     # Enqueue a job
#     test_queue.enqueue(add, args=(1, 2), on_success=BaseWorkClass.on_success)
    
#     # Shutdown workforce
#     workforce.shutdown()
    
#     # Check all workers were stopped
#     workers = BaseWorkClass.all(redis_conn)
#     assert len(workers) == 0
