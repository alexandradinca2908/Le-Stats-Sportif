from queue import Queue
from threading import Thread, Event, Lock
import time
import os
import multiprocessing
import data_ingestor

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker

        #  Number of threads in threadpool
        self.num_of_threads = 0
        if os.environ['TP_NUM_OF_THREADS'] is not None:
            self.num_of_threads = os.environ['TP_NUM_OF_THREADS'] 
        else:
            self.num_of_threads = multiprocessing.cpu_count()

        #  Thread pool
        self.threads = []
        for i in range(self.num_of_threads):
            self.threads.append(TaskRunner(i))
        
        #  Task queue
        self.task_queue = []

        #  Server status
        self.server_is_on = True
    
        #  Events
        self.data_ingestor_complete = Event()

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

    #  Add task to queue
    def submit_task(self, task):
        with self.task_queue_lock:
            self.task_queue.append(task)

class TaskRunner(Thread):
    def __init__(self, id):
        # TODO: init necessary data structures
        Thread.__init__(self)
        self.id = id

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            pass
