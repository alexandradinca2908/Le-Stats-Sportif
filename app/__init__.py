import os
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)

webserver.job_counter = 1

webserver.thread_pool = ThreadPool()
#webserver.thread_pool.start()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
webserver.thread_pool.set_data_ingestor()

from app import routes
