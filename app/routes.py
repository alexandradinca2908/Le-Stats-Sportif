"""This module is responsible of processing server requests"""

import os
import json

from flask import request, jsonify
from app import webserver

RESULTS_PATH = 'results/'

@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """Example endpoint definition"""
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)

    # Method Not Allowed
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """Retrieve results for specified job id"""

    # Notify logger
    webserver.logger.info("get_results request received by server")

    # Check if job_id is valid
    if int(job_id) < 1 or int(job_id) > webserver.job_counter:
        # Notify logger
        webserver.logger.info("get_results request failed with invalid job id %s",
                              job_id)

        return jsonify({
            'status': 'error',
            'reason': 'Invalid job id'
            })

    # Check if job_id is done and return the result
    filename = RESULTS_PATH + job_id +'.json'
    if os.path.exists(filename) and os.path.getsize(filename) > 0:
        with open(filename, 'r', encoding='utf-8') as file:
            res = json.load(file)

        # Notify logger
        webserver.logger.info("get_results request completed for job_%s",
                              job_id)

        return jsonify({
            'status': 'done',
            'data': res
        })

    # If not, notify logger and return running status
    webserver.logger.info("get_results request still running")
    return jsonify({'status': 'running'})

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """Return status for all jobs"""

    # Notify logger
    webserver.logger.info("jobs request received by server")

    # For each job, check if associated file is created
    # File exists == task done, else task is still running
    jobs_dict = {}
    for job in range(1, webserver.job_counter):
        filename = RESULTS_PATH + str(job) + '.json'
        if os.path.exists(filename):
            jobs_dict[f'job_id_{job}'] = {'status': 'done'}
        else:
            jobs_dict[f'job_id_{job}'] = {'status': 'running'}

    jobs_dict = dict(sorted(jobs.items()))

    return jsonify({
        'status': 'done',
        'data': jobs
    })

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """Return number of active tasks"""

    # Notify logger
    webserver.logger.info("num_jobs request received by server")

    return jsonify({
        'num_jobs': webserver.thread_pool.get_active_tasks()
    })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """Send states_mean request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("states_mean request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'states_mean'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("states_mean request received with question %s",
                           data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("states_mean request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """Send state_mean request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        webserver.logger.info("state_mean request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'state_mean'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("state_mean request received with question %s for state %s",
                           data['question'], data['state'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("state_mean request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """Send best5 request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("best5 request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'best5'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("best5 request received with question %s", data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("best5 request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """Send worst5 request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("worst5 request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'worst5'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("worst5 request received with question %s", data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("worst5 request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """Send global_mean request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("global_mean request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'global_mean'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("global_mean request received with question %s", data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("global_mean request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """Send diff_from_mean request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("diff_from_mean request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'diff_from_mean'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("diff_from_mean request received with question %s", data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("diff_from_mean request sent to task queue for job_%d", data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """Send state_diff_from_mean request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("state_diff_from_mean request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'state_diff_from_mean'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("state_diff_from_mean request received with question %s for state %s",
                           data['question'], data['state'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("state_diff_from_mean request sent to task queue for job_%d",
                          data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """Send mean_by_category request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("mean_by_category request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'mean_by_category'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("mean_by_category request received with question %s", data['question'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("mean_by_category request sent to task queue for job_%d",
                          data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """Send state_mean_by_category request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("state_mean_by_category request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = request.json
    data['task'] = 'state_mean_by_category'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("state_mean_by_category request received with question %s for state %s",
                           data['question'], data['state'])

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("state_mean_by_category request sent to task queue for job_%d",
                          data['job_id'])

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    """Send graceful_shutdown request to threadpool"""

    # If server is stopped, return error
    if not webserver.thread_pool.get_server_status():
        # Notify logger
        webserver.logger.info("graceful_shutdown request received but server has stopped")

        return jsonify({
            'status': 'error',
            'reason': 'shutting down'
        })

    # Get request data
    data = {}
    data['task'] = 'graceful_shutdown'
    data['job_id'] = webserver.job_counter

    # Notify logger
    webserver.logger.info("graceful_shutdown request received by server")

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Notify logger
    webserver.logger.info("graceful_shutdown request sent to task queue for job_%d",
                          data['job_id'])

    # Notify logger and return server status
    if webserver.thread_pool.get_active_tasks() == 0:
        webserver.logger.info("server status after graceful_shutdown: done")
        return jsonify({'status': 'done'})

    webserver.logger.info("server status after graceful_shutdown: running")
    return jsonify({'status': 'running'})

@webserver.route('/')
@webserver.route('/index')
def index():
    """You can check localhost in your browser to see what this displays"""

    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    """Returns all defined routes"""

    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
