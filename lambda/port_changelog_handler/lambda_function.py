# file: lambda_function.py
# lambda entrypoint: lambda_handler

import base64
import os
import logging
from typing import Union
import jsonpickle
import json
import requests
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CLIENT_ID = os.environ['PORT_CLIENT_ID']
CLIENT_SECRET = os.environ['PORT_CLIENT_SECRET']

CREATE_TRIGGER = 'CREATE'

API_URL = 'https://api.getport.io/v1'


def get_port_api_token():
    """
    Get a Port API access token
    This function uses CLIENT_ID and CLIENT_SECRET from config
    """

    credentials = {'clientId': CLIENT_ID, 'clientSecret': CLIENT_SECRET}

    token_response = requests.post(f"{API_URL}/auth/access_token", json=credentials)

    return token_response.json()['accessToken']


def update_entity_prop_value(blueprint_identifier: str, identifier: str, property_name: str, property_value: Union[str, int]):
    '''
    Patches a Port entity based on ``entity_props``
    '''
    logger.info('Fetching token')
    token = get_port_api_token()

    headers = {
        'Authorization': f'Bearer {token}'
    }

    entity = {
        'properties': {
            property_name: property_value
        }
    }

    logger.info('Updating entity property values:')
    logger.info(json.dumps(entity))
    response = requests.patch(f'{API_URL}/blueprints/{blueprint_identifier}/entities/{identifier}', json=entity, headers=headers)
    logger.info(response.status_code)
    logger.info(json.dumps(response.json()))


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

                change_type = message['action']
                resource_type = message['resourceType']

                # "message" includes one change that occurred in the service catalog
                # since all changes that happen in the catalog will trigger this Lambda, it would be a good idea to add separate handler
                # functions to keep code maintainable

                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
                # Your handler code for the changes in the catalog comes here #
                # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

                # Here is sample code to find the change in VM free storage space
                if change_type == 'UPDATE' and resource_type == 'entity':
                    blueprint_identifier = message['context']['blueprint']
                    entity_after_change_state = message['diff']['after']
                    entity_identifier = entity_after_change_state["identifier"]
                    entity_title = entity_after_change_state["title"]
                    entity_props_after_change = entity_after_change_state['properties']
                    entity_total_storage = entity_props_after_change['storage_size']
                    entity_free_storage = entity_props_after_change['free_storage']

                    if entity_total_storage * 0.1 > entity_free_storage:
                        logger.warning(f'Entity {entity_title} free storage is too low, fixing...')
                        # Assume a call to direct storage extensions using cloud provider SDK
                        # Or a call to some scheduled task that frees up storage on the VM
                        logger.info(f'Entity {entity_title} Storage freed up, updating in Port')
                        free_storage_after_cleanup = 4
                        update_entity_prop_value(blueprint_identifier, entity_identifier, 'free_storage', free_storage_after_cleanup)
            except Exception as e:
                traceback.print_exc()
                logger.warn(f'Error: {e}')
    return {"message": "ok"}


if __name__ == "__main__":
    pass
