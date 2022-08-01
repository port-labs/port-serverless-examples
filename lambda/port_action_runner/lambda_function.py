# file: lambda_function.py
# lambda entrypoint: lambda_handler

import base64
import os
import logging
import jsonpickle
import json
import requests
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CLIENT_ID = os.environ['PORT_CLIENT_ID']
CLIENT_SECRET = os.environ['PORT_CLIENT_SECRET']

CREATE_TRIGGER = 'CREATE'

API_URL = 'https://api.getport.io/v0.1'


def convert_status_code_to_run_status(status_code: int):
    if 200 <= status_code < 300:
        return "SUCCESS"
    if status_code >= 400:
        return "FAILURE"
    return "IN_PROGRESS"


def get_port_api_token():
    '''
    Get a Port API access token

    This function uses a global ``CLIENT_ID`` and ``CLIENT_SECRET``
    '''
    credentials = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

    token_response = requests.get(f'{API_URL}/auth/access_token', params=credentials)
    access_token = token_response.json()['accessToken']

    return access_token


def report_to_port(run_id: str ,entity_props: dict):
    '''
    Reports to Port on a new entity based on provided ``entity_props``
    '''
    logger.info('Fetching token')
    token = get_port_api_token()

    headers = {
        'Authorization': f'Bearer {token}'
    }

    params = {
      'run_id': run_id
    }

    entity = {
        'identifier': entity_props['title'].replace(' ', '-').lower(),
        'title': entity_props['title'],
        'blueprint': 'vm',
        'properties': {
            'cpu_cores': entity_props['cpu'],
            'memory_size': entity_props['memory'],
            'storage_size': entity_props['storage'],
            'region': entity_props['region'],
            'deployed': 'Deploying'
        }
    }

    logger.info('Creating entity:')
    logger.info(json.dumps(entity))
    response = requests.post(f'{API_URL}/entities', json=entity, headers=headers, params=params)
    logger.info(response.status_code)
    logger.info(json.dumps(response.json()))

    return response.status_code


def report_action_status(run_context: dict, status: str):
    '''
    Reports to Port on the status of an action run ``entity_props``
    '''
    logger.info('Fetching token')
    token = get_port_api_token()

    headers = {
        'Authorization': f'Bearer {token}'
    }

    run_id = run_context['runId']

    body = {
        "status": status,
        "message": {
            "message": f"The action status is {status}"
        }
    }

    logger.info(f'Reporting action {run_id} status:')
    logger.info(json.dumps(body))
    response = requests.patch(f'{API_URL}/actions/runs/{run_id}', json=body, headers=headers)
    logger.info(response.status_code)
    logger.info(json.dumps(response.json()))

    return response.status_code


def lambda_handler(event, context):
    '''
    Receives an event from AWS, if configured with a Kafka Trigger, the event includes an array of base64 encoded messages from the different topic partitions
    '''
    logger.info('## ENVIRONMENT VARIABLES\r' + jsonpickle.encode(dict(**os.environ)))
    logger.info('## EVENT\r' + jsonpickle.encode(event))
    logger.info('## CONTEXT\r' + jsonpickle.encode(context))
    for messages in event['records'].values():
        for encoded_message in messages:
            try:
                message_json_string = base64.b64decode(encoded_message['value']).decode('utf-8')
                logger.info('Received message:')
                logger.info(message_json_string)
                message = json.loads(message_json_string)

                run_id = message['context']['runId']

                # "message" includes one execution invocation object
                # You can use the message object as shown here to filter the handling of different actions you configured in Port
                action_type = message['payload']['action']['trigger']
                if action_type != CREATE_TRIGGER:
                    return {'message': 'Message not directed to our service'}

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Your handler code for the action execution comes here #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

                # All of the input fields you specified in the action invocation are available under message['payload']['properties']
                # For this example, we are simply invoking a simple reporter function which will send data about the new entity to Port
                status_code = report_to_port(run_id, message['payload']['properties'])
                report_action_status(run_id, convert_status_code_to_run_status(status_code))
            except Exception as e:
                traceback.print_exc()
                logger.warn(f'Error: {e}')
    return {"message": "ok"}


if __name__ == "__main__":
    pass
