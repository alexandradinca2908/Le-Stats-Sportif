from queue import Queue
from threading import Thread, Event, Lock, Condition
import time
import os
import multiprocessing
# from data_ingestor import DataIngestor

class ThreadPool:
    def __init__(self):
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker

        #  Number of threads in threadpool
        self.num_of_threads = int(os.environ.get("TP_NUM_OF_THREADS", multiprocessing.cpu_count()))

        #  Thread pool
        self.threads = []
        for i in range(self.num_of_threads):
            self.threads.append(TaskRunner(i, self))
        
        #  Task queue
        self.task_queue = []
    
        #  Events
        self.data_ingestor_complete = Event()
        self.server_running = Event()
        self.server_running.set()
    
        #  Conditions
        self.task_barrier = Condition()

        #  Locks
        self.task_queue_lock = Lock()
        self.data_ingestor_lock = Lock()
        self.results_lock = Lock()

    #  Starts thread pool
    def start(self):
        for i in range(self.num_of_threads):
            self.threads[i].start()
    
    #  Stops waits for threads to finish and closes server
    def stop(self):
        pass

    #  Notifies thread that they can start processing requests
    def set_data_ingestor(self):
        self.data_ingestor_complete.set()

    #  Add task to queue if server still accepts jobs
    def submit_task(self, task):
        if self.server_running.is_set():
            with self.task_queue_lock:
                self.task_queue.append(task)

class TaskRunner(Thread):
    def __init__(self, id, thread_pool):
        Thread.__init__(self)
        self.id = id
        self.thread_pool = thread_pool

    def run(self):
        #  Wait until CSV is loaded
        self.thread_pool.data_ingestor_complete.wait()

        while True:
            #  Wait for a task
            self.thread_pool.task_barrier.wait()

            #  Get pending task
            with self.thread_pool.task_queue_lock:
                task = self.thread_pool.task_queue.pop()

            if task == "graceful_shutdown":
                break
            elif task == "states_mean":
                pass
            elif task == "state_mean":
                pass
            elif task == "best5":
                pass
            elif task == "worst5":
                pass
            elif task == "global_mean":
                pass
            elif task == "diff_from_mean":
                pass
            elif task == "state_diff_from_mean":
                pass
            elif task == "mean_by_category":
                pass
            elif task == "state_mean_by_category":
                pass
            elif task == "diff_from_mean":
                pass

            # Execute the task and save the result to disk
            # Repeat until graceful_shutdown

