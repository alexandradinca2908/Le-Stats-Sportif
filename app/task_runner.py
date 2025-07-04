"""This module contains the app's thread pool and threads (TaskRunner)"""

from queue import Queue
from threading import Thread, Event, Lock, Semaphore
import time
import os
import multiprocessing
import json

RESULTS_PATH = 'results/'

class ThreadPool:
    """This is the thread pool class. It stores all the sync methods and data for threads."""

    def __init__(self, webserver):
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # Note: the TP_NUM_OF_THREADS env var will be defined by the checker

        # Webserver
        self.webserver = webserver

        # Number of threads in threadpool
        self.num_of_threads = int(os.environ.get('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))

        # Thread pool
        self.threads = []
        for i in range(self.num_of_threads):
            self.threads.append(TaskRunner(i, self))

        # Active tasks
        self.active_tasks = 0

        # Task queue
        self.task_queue = Queue()

        # Events
        self.data_ingestor_complete = Event()
        self.server_running = Event()
        self.server_running.set()

        # Semaphores
        self.task_barrier = Semaphore(0)

        # Locks
        self.active_tasks_lock = Lock()

    def set_data_ingestor(self):
        """Notifies thread that they can start processing requests"""
        self.data_ingestor_complete.set()

    def get_active_tasks(self):
        """Returns active tasks"""
        with self.active_tasks_lock:
            return self.active_tasks

    def get_server_status(self):
        """Returns server status"""
        return self.server_running.is_set()

    def start(self):
        """Starts thread pool"""
        for i in range(self.num_of_threads):
            self.threads[i].start()

    def stop(self, task):
        """Notifies the threads to finish by adding a stop task for each thread"""
        for _ in range(self.num_of_threads):
            self.task_queue.put(task)
            self.task_barrier.release()
            self.server_running.clear()

    def submit_task(self, task):
        """Add task to queue
        For graceful_shutdown, activate stop
        For normal task, notify threads and increment active tasks counter"""
        if self.server_running.is_set():
            if task['task'] == 'graceful_shutdown':
                self.stop(task)
            else:
                self.task_queue.put(task)
                self.task_barrier.release()

                with self.active_tasks_lock:
                    self.active_tasks += 1

class TaskRunner(Thread):
    """This is the thread class. Each thread runs tasks from thread pool queue until told to stop."""

    def __init__(self, id, thread_pool):
        Thread.__init__(self)
        self.id = id
        self.thread_pool = thread_pool

    def run(self):
        """Run function for a TaskRunner thread"""
        # Wait until CSV is loaded
        self.thread_pool.data_ingestor_complete.wait()

        while True:
            # Wait for a task
            self.thread_pool.task_barrier.acquire()

            # Get pending task
            task = self.thread_pool.task_queue.get()

            # Execute the task and save the result to disk
            # Repeat until graceful_shutdown
            if task['task'] == 'graceful_shutdown':
                break

            if task['task'] == 'states_mean':
                self.states_mean(task)
            elif task['task'] == 'state_mean':
                self.state_mean(task)
            elif task['task'] == 'best5':
                self.best5(task)
            elif task['task'] == 'worst5':
                self.worst5(task)
            elif task['task'] == 'global_mean':
                self.global_mean(task)
            elif task['task'] == 'diff_from_mean':
                self.diff_from_mean(task)
            elif task['task'] == 'state_diff_from_mean':
                self.state_diff_from_mean(task)
            elif task['task'] == 'mean_by_category':
                self.mean_by_category(task)
            elif task['task'] == 'state_mean_by_category':
                self.state_mean_by_category(task)

            # Decrement active tasks counter
            with self.thread_pool.active_tasks_lock:
                self.thread_pool.active_tasks -= 1

    def states_mean(self, task, write = True, csv_file = None):
        """Executes states mean request"""
        # Dictionary for all states
        # states[state] = (sum of values, nr of values)
        states = {}

        # Extract only needed rows
        if csv_file is None:
            csv_file = self.thread_pool.webserver.data_ingestor.csv_file

        filtered_csv = csv_file[csv_file['Question'] == task['question']]

        for _, row in filtered_csv.iterrows():
            # Take state
            state = row['LocationDesc']

            # Add new data
            if state not in states:
                states[state] = (row['Data_Value'], 1)
            # Add data to existing data
            else:
                tuple1 = states[state]
                tuple2 = (row['Data_Value'], 1)

                states[state] = tuple(sum(x) for x in zip(tuple1, tuple2))

        # Take all states and calculate the mean values
        result_list = []
        for state in states:
            sum_of_values = states[state][0]
            nr_of_values = states[state][1]

            result_list.append((state, sum_of_values / nr_of_values))

        # Sort values by mean value
        result_list = dict(sorted(result_list, key = lambda x : x[1]))

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(result_list))

        return result_list

    def state_mean(self, task, write = True, csv_file = None):
        """Executes state mean request"""
        # Extract only needed rows
        if csv_file is None:
            csv_file = self.thread_pool.webserver.data_ingestor.csv_file

        filtered_csv = csv_file[(csv_file['Question'] == task['question']) &
                               (csv_file['LocationDesc'] == task['state'])]

        state = (0, 0)

        for _, row in filtered_csv.iterrows():
            # Add new data
            tuple1 = state
            tuple2 = (row['Data_Value'], 1)

            state = tuple(sum(x) for x in zip(tuple1, tuple2))

        # Calculate the mean values
        sum_of_values = state[0]
        nr_of_values = state[1]

        result = {task['state']: sum_of_values / nr_of_values}

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(result))

        return result

    def best5(self, task, write = True, data_ingestor = None):
        """Executes best5 request"""
        # Extract data ingestor
        if data_ingestor is None:
            data_ingestor = self.thread_pool.webserver.data_ingestor

        # Generate all results
        full_result = self.states_mean(task, write = False, csv_file = data_ingestor.csv_file)

        # Extract best 5
        best5 = {}
        if task['question'] in data_ingestor.questions_best_is_min:
            best5 = {k: full_result[k] for k in list(full_result)[:5]}
        else:
            best5 = {k: full_result[k] for k in list(full_result)[-5:]}

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(best5))

        return best5

    def worst5(self, task, write = True, data_ingestor = None):
        """Executes worst5 request"""
        # Extract data ingestor
        if data_ingestor is None:
            data_ingestor = self.thread_pool.webserver.data_ingestor

        # Generate all results
        full_result = self.states_mean(task, write = False, csv_file = data_ingestor.csv_file)

        # Extract worst 5
        worst5 = {}
        if task['question'] in data_ingestor.questions_best_is_max:
            worst5 = {k: full_result[k] for k in list(full_result)[:5]}
        else:
            worst5 = {k: full_result[k] for k in list(full_result)[-5:]}

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(worst5))

        return worst5

    def global_mean(self, task, write = True, csv_file = None):
        """Executes global mean request"""
        # Extract only needed rows
        if csv_file is None:
            csv_file = self.thread_pool.webserver.data_ingestor.csv_file

        filtered_csv = csv_file[csv_file['Question'] == task['question']]

        values = (0, 0)

        for _, row in filtered_csv.iterrows():
            # Add new data
            tuple1 = values
            tuple2 = (row['Data_Value'], 1)

            values = tuple(sum(x) for x in zip(tuple1, tuple2))

        # Calculate the mean values
        sum_of_values = values[0]
        nr_of_values = values[1]

        result = {'global_mean': sum_of_values / nr_of_values}

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(result))

        return result

    def diff_from_mean(self, task, write = True, csv_file = None):
        """Executes diff_from_mean request"""
        states_mean = self.states_mean(task, write = False, csv_file = csv_file)
        global_mean = self.global_mean(task, write = False, csv_file = csv_file)['global_mean']

        for state in states_mean:
            states_mean[state] = global_mean - states_mean[state]

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(states_mean))

        return states_mean

    def state_diff_from_mean(self, task, write = True, csv_file = None):
        """Executes state_diff_from_mean request"""
        state_mean = self.state_mean(task, write = False, csv_file = csv_file)
        global_mean = self.global_mean(task, write = False, csv_file = csv_file)['global_mean']
        state = task['state']

        state_mean[state] = global_mean - state_mean[state]

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(state_mean))

        return state_mean

    def mean_by_category(self, task, write = True, csv_file = None):
        """Executes mean_by_category request"""
        # Dictionary for all states
        # states[state] = (sum of values, nr of values)
        states = {}

        # Extract only needed rows
        if csv_file is None:
            csv_file = self.thread_pool.webserver.data_ingestor.csv_file

        filtered_csv = csv_file[(csv_file['Question'] == task['question']) &
                                (csv_file['StratificationCategory1'].notna()) &
                                (csv_file['Stratification1'].notna())]

        for _, row in filtered_csv.iterrows():
            # Take state
            state = (row['LocationDesc'], row['StratificationCategory1'],
                    row['Stratification1'])

            # Add new data
            if state not in states:
                states[state] = (row['Data_Value'], 1)
            # Add data to existing data
            else:
                tuple1 = states[state]
                tuple2 = (row['Data_Value'], 1)

                states[state] = tuple(sum(x) for x in zip(tuple1, tuple2))

        # Take all states and calculate the mean values
        result_list = []
        for state in states:
            sum_of_values = states[state][0]
            nr_of_values = states[state][1]

            result_list.append((state, sum_of_values / nr_of_values))

        # Sort values by state name, stratification category and stratification
        result_list = dict(sorted(result_list,
                                  key = lambda x : (str(x[0][0]), str(x[0][1]), str(x[0][2]))))

        # Replace tuple keys with string keys
        final_result = {}
        for result in result_list:
            key = '(\'' + str(result[0]) + '\', \'' + str(result[1]) + '\', \'' + str(result[2]) + '\')'
            final_result[key] = result_list[result]

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(final_result))

        return result_list

    def state_mean_by_category(self, task, write = True, csv_file = None):
        """Executes state_mean_by_category request"""
        # Dictionary for all states
        # states[state] = (sum of values, nr of values)
        states = {}

        # Extract only needed rows
        if csv_file is None:
            csv_file = self.thread_pool.webserver.data_ingestor.csv_file

        filtered_csv = csv_file[(csv_file['Question'] == task['question']) &
                                (csv_file['LocationDesc'] == task['state']) &
                                (csv_file['StratificationCategory1'].notna()) &
                                (csv_file['Stratification1'].notna())]

        for _, row in filtered_csv.iterrows():
            # Take state
            state = (row['StratificationCategory1'], row['Stratification1'])

            # Add new data
            if state not in states:
                states[state] = (row['Data_Value'], 1)
            # Add data to existing data
            else:
                tuple1 = states[state]
                tuple2 = (row['Data_Value'], 1)

                states[state] = tuple(sum(x) for x in zip(tuple1, tuple2))

        # Take all state values and calculate the mean values
        result_list = []
        for state in states:
            sum_of_values = states[state][0]
            nr_of_values = states[state][1]

            result_list.append((state, sum_of_values / nr_of_values))

        # Sort values by stratification category and stratification
        result_list = dict(sorted(result_list, key = lambda x : (str(x[0][0]), str(x[0][1]))))

        # Replace tuple keys with string keys
        formatted_result = {}
        for result in result_list:
            key = '(\'' + str(result[0]) + '\', \'' + str(result[1]) + '\')'
            formatted_result[key] = result_list[result]

        # Create final dictionary: State name: {formatted_result}
        final_result = {task['state']: formatted_result}

        # Write output in result file
        if write:
            filename = RESULTS_PATH + str(task['job_id']) + '.json'
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json.dumps(final_result))

        return result_list
