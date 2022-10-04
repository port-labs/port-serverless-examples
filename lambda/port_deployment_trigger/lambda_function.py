# file: lambda_function.py
# lambda entrypoint: lambda_handler

from github import Github, Repository
import os
import logging
import jsonpickle
import json
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CLIENT_ID = os.environ['PORT_CLIENT_ID']
CLIENT_SECRET = os.environ['PORT_CLIENT_SECRET']
GITHUB_MACHINE_TOKEN = os.environ['GITHUB_MACHINE_TOKEN']

GITHUB_ORG = 'port-labs'

TARGET_REPO = 'resource-catalog-microservice-repo'

TARGET_WORKFLOW = 'Deploy Recommendation Prod'

API_URL = 'https://api.getport.io/v1'

g = Github(GITHUB_MACHINE_TOKEN)


def get_target_repo():
    return g.get_organization(GITHUB_ORG).get_repo(TARGET_REPO)


def get_target_workflow(repo: Repository):
    for workflow in repo.get_workflows():
        print(workflow.name)
        if workflow.name == TARGET_WORKFLOW:
            print("Found")
            return workflow


def get_port_api_token():
    """
    Get a Port API access token
    This function uses CLIENT_ID and CLIENT_SECRET from config
    """

    credentials = {'clientId': CLIENT_ID, 'clientSecret': CLIENT_SECRET}

    token_response = requests.post(f"{API_URL}/auth/access_token", json=credentials)

    return token_response.json()['accessToken']


def report_action_status(run_id: str, status: str):
    '''
    Reports to Port on the status of an action run ``entity_props``
    '''
    logger.info('Fetching token')
    token = get_port_api_token()

    headers = {
        'Authorization': f'Bearer {token}'
    }

    body = {
        "status": status,
        "message": {
            "message": f"The deploy workflow has been triggered"
        }
    }

    logger.info(f'Reporting action {run_id} status:')
    logger.info(json.dumps(body))
    response = requests.patch(f'{API_URL}/actions/runs/{run_id}', json=body, headers=headers)
    logger.info(response.status_code)
    logger.info(json.dumps(response.json()))

    return response.status_code


def lambda_handler(event, context):
    logger.info('## ENVIRONMENT VARIABLES\r' + jsonpickle.encode(dict(**os.environ)))
    logger.info('## EVENT\r' + jsonpickle.encode(event))
    logger.info('## CONTEXT\r' + jsonpickle.encode(context))

    req_body = json.loads(event['body'])
    run_id = req_body['context']['runId']
    repo = get_target_repo()
    main_branch = repo.get_branch('main')
    print(main_branch)
    workflow = get_target_workflow(repo)
    print(workflow)
    result = workflow.create_dispatch(main_branch, {})
    print(f'Got: {result}')

    report_action_status(run_id, 'SUCCESS')

    return {
        "body": {
            "message": "Workflow triggered"
        },
        "statusCode": 200
    }


if __name__ == "__main__":
    pass
