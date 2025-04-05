from app import webserver
from flask import request, jsonify

import os
import json

RESULTS_PATH = 'results/'

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")

    # Check if job_id is valid
    if int(job_id) < 1 or int(job_id) > webserver.job_counter:
        return jsonify({
            'status': 'error',
            'reason': 'Invalid job id'
            })
    
    # Check if job_id is done and return the result
    filename = RESULTS_PATH + job_id
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            res = json.load(file)
        
        return jsonify({
            'status': 'done',
            'data': res
        })

    # If not, return running status
    else:
        return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    data['task'] = 'states_mean'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # Get request data
    data = request.json
    data['task'] = 'state_mean'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # Get request data
    data = request.json
    data['task'] = 'best5'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # Get request data
    data = request.json
    data['task'] = 'worst5'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # Get request data
    data = request.json
    data['task'] = 'global_mean'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # Get request data
    data = request.json
    data['task'] = 'diff_from_mean'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # Get request data
    data = request.json
    data['task'] = 'state_diff_from_mean'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # Get request data
    data = request.json
    data['task'] = 'mean_by_category'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # Get request data
    data = request.json
    data['task'] = 'state_mean_by_category'
    data['job_id'] = webserver.job_counter

    # Register job. Don't wait for task to finish
    webserver.thread_pool.submit_task(data)

    # Increment job_id counter
    webserver.job_counter += 1

    # Return associated job_id
    return jsonify({'job_id': data['job_id']})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    return jsonify({"status": "NotImplemented"})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
