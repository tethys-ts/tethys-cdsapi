#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 11:31:50 2025

@author: mike
"""
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import urllib.parse
from urllib3.util import Retry, Timeout
import shutil
import orjson

############################################
### Parameters

product = 'reanalysis-era5-land'
base_url = 'https://cds.climate.copernicus.eu/api'
chunk_size = 2**21

req_url = f'{base_url}/resources/{product}'

jobs_url = f'{base_url}/retrieve/v1/jobs?limit=100'

headers = {
    'User-Agent': 'cads-api-client/1.5.2',
    'PRIVATE-TOKEN': f'{key}'
    }

headers = {
    'PRIVATE-TOKEN': f'{key}'
    }

job_id = '993a2b8f-b885-40c3-9d70-56d9273f199f' # Completed
job_id = 'deeea101-0c69-4748-91cf-531c0a1b68c2' # running
job_id = 'faa6bc39-fa23-4218-91a9-9f4dec2c3ab3' # Failed
job_id = '47976e7a-fd17-4b85-b060-00a78e6f70fa' # accepted

job_status_url = f'{base_url}/retrieve/v1/jobs/{job_id}'
job_results_url = f'{base_url}/retrieve/v1/jobs/{job_id}/results'

delete_url = f'{base_url}/retrieve/v1/jobs/{job_id}'

# request_url = f'{base_url}/resources/{product}'

request_url = f'{base_url}/retrieve/v1/processes/{product}/execute/'

request_dict = {'data_format': 'netcdf',
 'variable': '2m_dewpoint_temperature',
 'area': [-34.3, 166.3, -47.3, 178.6],
 'download_format': 'unarchived',
 'year': ['1950'],
 'month': ['01'],
 'day': ['01'],
 'time': ['00:00',
  '01:00',
  '02:00',
  '03:00',
  '04:00',
  '05:00',
  '06:00',
  '07:00',
  '08:00',
  '09:00',
  '10:00',
  '11:00',
  '12:00',
  '13:00',
  '14:00',
  '15:00',
  '16:00',
  '17:00',
  '18:00',
  '19:00',
  '20:00',
  '21:00',
  '22:00',
  '23:00']}

############################################
### Functions


def session(max_pool_connections: int = 10, max_attempts: int=3, timeout: int=120):
    """
    Function to setup a urllib3 pool manager for url downloads.

    Parameters
    ----------
    max_pool_connections : int
        The number of simultaneous connections for the S3 connection.
    max_attempts: int
        The number of retries if the connection fails.
    timeout: int
        The timeout in seconds.

    Returns
    -------
    Pool Manager object
    """
    timeout = urllib3.util.Timeout(timeout)
    retries = Retry(
        total=max_attempts,
        backoff_factor=1,
        )
    http = urllib3.PoolManager(num_pools=max_pool_connections, timeout=timeout, retries=retries)

    return http

############################################
### Testing

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
sess.mount('https://', adapter)

auth = HTTPBasicAuth('', key)
# sess.auth = (key,)
sess.headers = {'User-Agent': 'cads-api-client/1.5.2',}



sess = session()


## Get all jobs
resp = sess.request('get', jobs_url, headers=headers)

jobs = resp.json()['jobs']

for job in jobs:
    # print(job['status'])
    # if job['jobID'] == job_id:
    #     d
    if job['status'] == 'failed':
        d

## Get individual jobs and results
resp = sess.request('get', job_stats_url, headers=headers)
job = resp.json()

resp = sess.request('get', job_results_url, headers=headers)
job_results = resp.json()

## Delete job
resp = sess.request('delete', delete_url, headers=headers)
job_delete = resp.json()

## Download file
for job in jobs:
    if job['jobID'] == job_id:
        download_url = job['metadata']['results']['asset']['value']['href']

resp = sess.request('get', download_url, preload_content=False)

with open('/home/mike/data/ecmwf/cache/test.nc', 'wb') as f:
    shutil.copyfileobj(resp, f, chunk_size)
    # chunk = resp.read(chunk_size)
    # while chunk:
    #     f.write(chunk)
    #     chunk = resp.read(chunk_size)

resp.release_conn()


## Request dataset
resp = sess.request('post', request_url, json={'inputs': request_dict}, headers=headers)
print(resp.status)
print(resp.json())

resp = sess.post(request_url, json={'inputs': request_dict}, headers=headers)
print(resp.content)

resp = requests.get(job_status_url, headers=headers)
job_status = resp.json()

resp = requests.get(jobs_url, headers=headers)
jobs_status = resp.json()['jobs']

job = [j for j in jobs_status if j['jobID'] == job_id]

resp = requests.get(job_results_url, headers=headers)
job_results = resp.json()

resp = requests.get('https://object-store.os-api.cci2.ecmwf.int:443/cci2-prod-cache-1/2025-03-14/d354f0e372f931bb7965a3c189a4d67d.nc')





































































