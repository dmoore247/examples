from collections import namedtuple
from typing import Tuple
from click.termui import prompt
from numpy import longcomplex
from pandas.core.computation.pytables import Constant
import requests
from requests.models import Response
from urllib3 import response
import json
import time
import click
import logging


"""
    Code to direct upload to S3 by generating signed URL using a cluster with a valide instance profile.
"""

API_EXECUTE = "/api/1.2/commands/execute"
API_STATUS  = "/api/1.2/commands/status"
API_CREATE  = "/api/1.2/contexts/create"

# define helper functions
def get_url(domain:str, api:str, org_id:int=0) -> str:
    if org_id != 0:
        return F"https://{domain}{api}?o={org_id}"
    else:
        return F"https://{domain}{api}"

def db_command_create_context(url:str, clusterId:str, language:str, token:str) -> str:
    """ Create databricks execution context (session) """
    headers={'Authorization': F'Bearer {token}'}
    res = requests.post(
        url,
        headers=headers,
        json={'clusterId':clusterId, 'language':language}
    )
    payload = res.json()
    contextId = payload['id']
    return contextId

def db_command_execute(domain, clusterId, language, token, contextId, remote_command) -> str:
    url = get_url(domain,API_EXECUTE)
    logging.debug(F'running remote_command\n{remote_command}')
    headers={'Authorization': F'Bearer {token}'}
    response = requests.post(
        url,
        headers=headers,
        json={
            'clusterId':clusterId,
            'language':language,
            'contextId':contextId,
            'command': remote_command
            }
    )
    payload = response.json()
    commandId = payload['id']
    return commandId

def db_command_wait_response(domain, clusterId, token, contextId, commandId, poll_period = 0.5) -> dict:
    response = {}
    headers={'Authorization': F'Bearer {token}'}
    while True:
        url = get_url(domain,API_STATUS)
        res = requests.get(
            F"{url}?clusterId={clusterId}&contextId={contextId}&commandId={commandId}",
            headers=headers
        )
        response = res.json()
        if response['status'] == 'Finished':
            break
        time.sleep(poll_period)
    return response

def print_url(r:Response, *args, **kwargs):
    logging.debug(f"URL: {r.url}, \nElapsed: {r.elapsed}, \nStatus: {r.status_code}, \nHeaders: {r.headers}, \nEncoding: {r.encoding}")

def s3_upload(signed_url:str, file_name:str, bucket_name:str, object_name:str, fields) -> str:
    """ Upload file to S3 using signed URL """
    hooks={'response':  print_url}
    with open(file_name, 'rb') as f:
        files = {"file": (object_name, f)}
        logging.info(F"uploading {file_name} to s3a://{bucket_name}/{object_name}....")
        logging.debug(F"{signed_url}")
        http_response = requests.post(signed_url, hooks=hooks, data=fields, files=files, stream=True, verify=True)
        if http_response.status_code == 204:
            logging.info("upload successful.")
        else:
            logging.error(F"problem: {http_response.status_code} {http_response.text}")



def parse_cluster_url(cluster_url:str) -> Tuple[str,int,str]:
    parts = cluster_url.split('/')[2:-1]
    del parts[2]
    
    host = None
    if parts[0].endswith('cloud.databricks.com'):
        host=parts[0]
    org_id = 0
    if parts[1].startswith('o='):
        org_id = parts[1][4:-8]
    cluster_id = parts[2]
    assert host.endswith('cloud.databricks.com')
    assert len(cluster_id.split('-')) == 3, F"{cluster_id} does not have format nnnn-nnnnnn-aaaaaa"
    return (host,org_id,cluster_id)


@click.command()
@click.option('--debug', is_flag=True, flag_value=True, default=False)
@click.option('--host', default='https://<yourhost>.cloud.databricks.com', help='The full Databricks workspace host url, starting with https://')
@click.option('--org_id', default=0, help='the org_id of your databricks deployment (the number following the o=) for multi-tenant deployments')
@click.option('--cluster_id',prompt='Your cluster id that has IAM write access to the S3 bucket')
@click.option('--token', prompt='personal access token (PAT)', help='Your Databricks Personal Access token')
@click.option('--file_name', prompt='local file to upload')
@click.option('--target', prompt='target s3 bucket and folder', help="The cluster must have write access to this bucket")
def run(host, cluster_id, token:str, file_name:str, target:str, debug:bool, org_id:int = 0, expires:int=300):
    """
    Purpose:    Direct upload object to S3 using Databricks Personal Access Token and a small cluster as token generator
                This command generates a short lived signed URL using a secured databricks cluster that has write access to the S3 bucket.
                The signed URL is used to upload the object to S3.

    Usage:
        python3 aws-upload.py --help

    Example:
        export DATABRICKS_TOKEN=dapi012345678THISISFAKETOKENdef0
        export DATABRICKS_CLUSTER_ID=0318-151752-abed99
        export DATABRICKS_HOST=https://demo.cloud.databricks.com
        python3 aws-upload.py --target=s3a://mybucket/myfolder/uploads --file_name=myfile.txt
    
    """
    assert host is not None
    assert cluster_id is not None
    assert target is not None
    assert file_name is not None

    print (debug)
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    s3_parts = target.split("://")[1].split('/')
    bucket_name = s3_parts[0]
    object_name = '/'.join(s3_parts[1:])
    if not object_name.endswith(file_name):
        object_name = F"{object_name}/{file_name}"
    
    domain = host.split('//')[1]
    language = "python"
    remote_command = F"import boto3\nprint(boto3.client('s3').generate_presigned_post('{bucket_name}','{object_name}', ExpiresIn={expires}))"
    url = get_url(domain,API_CREATE, org_id)
    logging.debug(F"{host}, {cluster_id}, {s3_parts}, {bucket_name}, {object_name}")
    exit
    contextId = db_command_create_context(url, cluster_id, language, token)
    commandId = db_command_execute(domain, cluster_id, language, token, contextId, remote_command)
    response  = db_command_wait_response(domain, cluster_id, token, contextId, commandId)
    data = json.loads(response['results']['data'].replace('\'','\"'))
    # convert to legit JSON
    signed_url = data['url']
    fields = data['fields']
    s3_upload(signed_url, file_name, bucket_name, object_name, fields)


if "__main__" == __name__:
    run(auto_envvar_prefix='DATABRICKS')  
