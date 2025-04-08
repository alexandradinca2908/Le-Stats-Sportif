"""This module initializes the Flask app and sets up routes."""

import os
import logging
import logging.handlers
import queue
import time
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)

webserver.job_counter = 1

webserver.thread_pool = ThreadPool(webserver)
webserver.thread_pool.start()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
webserver.thread_pool.set_data_ingestor()

def init_logger():
    """Initializes a logger with proper formatting to keep track of the app."""

    # Create logger
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    # Create the queue for records
    log_queue = queue.Queue()

    # Create rotating file handler
    handler = logging.handlers.RotatingFileHandler('file.log', maxBytes = 50000, backupCount = 10)

    # Set formatter in gmtime
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S')
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    # Add handler
    logger.addHandler(handler)

    # Create and start a QueueListener
    listener = logging.handlers.QueueListener(log_queue, handler)
    listener.start()

    return logger

webserver.logger = init_logger()

from app import routes
