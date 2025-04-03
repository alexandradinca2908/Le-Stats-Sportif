from queue import Queue
from threading import Thread, Event, Lock, Semaphore
import time
import os
import multiprocessing
import json

RESULTS_PATH = 'results/'

class ThreadPool:
    def __init__(self, webserver):
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker

        #  Webserver
        self.webserver = webserver

        #  Number of threads in threadpool
        self.num_of_threads = int(os.environ.get('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))

        #  Thread pool
        self.threads = []
        for i in range(self.num_of_threads):
            self.threads.append(TaskRunner(i, self))
        
        #  Task queue
        self.task_queue = Queue()
    
        #  Events
        self.data_ingestor_complete = Event()
        self.server_running = Event()
        self.server_running.set()
    
        #  Semaphore
        self.task_barrier = Semaphore(0)

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
    #  When receiving graceful_shutdown, send it to all threads
    def submit_task(self, task):
        if self.server_running.is_set():
            if task['task'] == 'graceful_shutdown':
                for i in range(self.num_of_threads):
                    self.task_queue.put(task)
                    self.task_barrier.release()
            else:
                self.task_queue.put(task)                
                self.task_barrier.release()

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
            self.thread_pool.task_barrier.acquire()

            #  Get pending task
            task = self.thread_pool.task_queue.get()                

            #  Execute the task and save the result to disk
            #  Repeat until graceful_shutdown
            if task['task'] == 'graceful_shutdown':
                break
            elif task['task'] == 'states_mean':
                self.states_mean(task)
            elif task['task'] == 'state_mean':
                self.state_mean(task)
            elif task['task'] == 'best5':
                pass
            elif task['task'] == 'worst5':
                pass
            elif task['task'] == 'global_mean':
                pass
            elif task['task'] == 'diff_from_mean':
                pass
            elif task['task'] == 'state_diff_from_mean':
                pass
            elif task['task'] == 'mean_by_category':
                pass
            elif task['task'] == 'state_mean_by_category':
                pass
            elif task['task'] == 'diff_from_mean':
                pass

        print("done\n")

    def states_mean(self, task):
        #  Dictionary for all states
        #  states[state] = (sum of values, nr of values)
        states = {}

        #  Extract only needed rows
        csvFile = self.thread_pool.webserver.data_ingestor.csvFile
        filtered_csv = csvFile[csvFile['Question'] == task['question']]

        for _, row in filtered_csv.iterrows():
            #  Take state
            state = row['LocationDesc']

            #  Add new data
            if state not in states:
                states[state] = (row['Data_Value'], 1)
            #  Add data to existing data
            else:
                tuple1 = states[state]
                tuple2 = (row['Data_Value'], 1)
    
                states[state] = tuple(sum(x) for x in zip(tuple1, tuple2))
        
        #  Take all states and calculate the mean values
        result_list = []
        for state in states:
            sum_of_values = states[state][0]
            nr_of_values = states[state][1]

            result_list.append((state, sum_of_values / nr_of_values))

        #  Sort values by mean value
        result_list = dict(sorted(result_list, key = lambda x : x[1]))

        #  Write output in result file
        filename = RESULTS_PATH + str(task['job_id']) + '.json'
        with open(filename, 'w') as file:
            file.write(json.dumps(result_list))

    def state_mean(self, task):
        #  Dictionary for all states
        #  states[state] = (sum of values, nr of values)
        states = {}

        #  Extract only needed rows
        csvFile = self.thread_pool.webserver.data_ingestor.csvFile
        filtered_csv = csvFile[(csvFile['Question'] == task['question']) &
                               (csvFile['LocationDesc'] == task['state'])]

        state = (0, 0)

        for _, row in filtered_csv.iterrows():
            #  Add new data
            tuple1 = state
            tuple2 = (row['Data_Value'], 1)

            state = tuple(sum(x) for x in zip(tuple1, tuple2))

        #  Calculate the mean values
        sum_of_values = state[0]
        nr_of_values = state[1]

        result = {task['state']: sum_of_values / nr_of_values}

        #  Write output in result file
        filename = RESULTS_PATH + str(task['job_id']) + '.json'
        with open(filename, 'w') as file:
            file.write(json.dumps(result))