# tethys-cdsapi

## cdsapi
The official python package (cdsapi) is quite complicated and pretty hard to read even for a python programmer like myself. 
For an equivalent set of code that is meant to mimic the python package, I would refer people to the Julia implementation:
[cdsapi.jl](https://github.com/JuliaClimate/CDSAPI.jl/blob/master/src/CDSAPI.jl)

It's much more straightforward to read and understand regardless of your programming language background.

## HTTP API
### Base url
The base url that is used for all http API calls is currently the following:
```
https://cds.climate.copernicus.eu/api
```

### Authentication
All http API calls must contain the PRIVATE-TOKEN header with the user-specific key. In python, the headers dictionary would be the following:
```
headers = {'PRIVATE-TOKEN': {key}}
```

### ECMWF Products
The product is the name the ECMWF gives to a dataset. For example, this can be:
```
product = 'reanalysis-era5-pressure-levels'
```
or
```
product = 'reanalysis-era5-land'
```

### Request parameters
The last piece of information you need to create a job on the CDS server is the request parameters specifically associated with a product. An easy way to figure this out is to go to the CDS website and manually click on variables, dates, times, etc and then go down to the "API Request" section at the very bottom to see the code.
Here's a link to a product download page:
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=download

Here's an example written as a python dictionary:
```
request_params = {
 'data_format': 'netcdf',
 'variable': '2m_dewpoint_temperature',
 'area': [-34.3, 166.3, -47.3, 178.6],
 'download_format': 'unarchived',
 'year': ['1950'],
 'month': ['01'],
 'day': ['01'],
 'time': ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00']
 }
```

If you set the data_format to 'netcdf', there's no need to have the 'download_format' set to 'zip' as CDS will use zip internally in the netcdf4 anyway (which is good). It does not appear that 'grib' uses internal compression by default, so you might want to use 'zip' in this case.

The number of data values that the CDS will process in one job changes from time to time. I find it good practice to request one variable for a maximum of one year at a time.

### Creating a job
Now that we have all the necessary pieces of data, we can create a job on the CDS server.
To create a job for the CDS server, we need to send a POST request to the following url template:
```
request_url = '{base_url}/retrieve/v1/processes/{product}/execute/'
```
In our above example, it would equate to:
```
request_url = 'https://cds.climate.copernicus.eu/api/retrieve/v1/processes/reanalysis-era5-land/execute/'
```
The request parameters must be sent as JSON in the body of the POST request, but with the addition of 'inputs' as the key for the entire request parameters.
For example, the python dictionary that should be converted to JSON should be the following using the above request_params:
```
request_body = {'inputs': request_params}
```
All up, here's an example of using the python requests package to create a job on the CDS server:
```
resp = requests.post(request_url, json=request_body, headers=headers)
```
#### Response
A response with a 404 error will either mean that the url is incorrect or the product name is wrong. A 400/500 error will mean that the request parameters were wrong.

The response headers will have two vital pieces of data. The JobID is the id the server has assigned for this job request and you should keep this for later! 

The second is the status. The status has four options: accepted, running, successful, and failed. When you first create a job you'll likely either get accepted or failed. The failed status can happen for various reasons. The ECMWF constantly changes the limits on the number of queued jobs per user, and they don't provide a clear way to know what that limit is. If the job has a status of accepted, then your good to go.

### Check the status of jobs
There are two ways to check the status of jobs on the CDS server.

#### Check the status of a single job
Using the job_id you retrieved from your POST request, you can see the status of that job by making a GET request to the following url:
```
job_status_url = '{base_url}/retrieve/v1/jobs/{job_id}'
```
In our example, it would be:
```
job_id = '993a2b8f-b885-40c3-9d70-56d9273f199f'
job_status_url = 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/993a2b8f-b885-40c3-9d70-56d9273f199f'
```
Using the python requests package, we would do the following: 
```
resp = requests.get(job_status_url, headers=headers)
```
This response is nearly identical to the response when creating a job via the POST request. When the status has changed to 'successful', then the download is ready. You'll then need to use the 'results' link to find the download link, which will be described in the next section.

Here's an example of a job status response (as a python dict):
```
response_dict = {
 'processID': 'reanalysis-era5-land',
 'type': 'process',
 'jobID': '993a2b8f-b885-40c3-9d70-56d9273f199f',
 'status': 'successful',
 'created': '2025-03-14T23:45:27.885965',
 'started': '2025-03-14T23:45:34.050973',
 'finished': '2025-03-14T23:45:35.915181',
 'updated': '2025-03-14T23:45:35.915181',
 'links': [{'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/993a2b8f-b885-40c3-9d70-56d9273f199f',
   'rel': 'self',
   'type': 'application/json'},
  {'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/993a2b8f-b885-40c3-9d70-56d9273f199f/results',
   'rel': 'results'}],
 'metadata': {'origin': 'api'}
 }
```

#### Check the status of all jobs
The other option is to check the status of all jobs.

This requires sending a GET request to the following url:
```
jobs_status_url = '{base_url}/retrieve/v1/jobs?limit=100'
```
which equates to:
```
jobs_status_url = 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs?limit=100'
```
Unlike other http requests, this one actually has some parameters that can be added to the GET request. The 'limit' parameter sets the max number of jobs that the server will provide. The default is 10, so I tend to put 100 to make sure I get all my jobs. The other parameter is 'status'. By default, the server will reply with jobs with all statuses. You can filter out certain status' by specifying the statuses in the url.

For example, the following url will only provide jobs that have a status of completed and running:
```
jobs_status_url = 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs?limit=100&status=completed&status=running'
```
The response is similar to the single job status request except that it's a list of JSON objects for each job. Each job also has more information. Instead of having to make a job status call then a job results call, this response has got it all!

Here's an example of a response (as a python list of dict):
```
rsponse_dict = [{
  'processID': 'reanalysis-era5-land',
  'type': 'process',
  'jobID': '993a2b8f-b885-40c3-9d70-56d9273f199f',
  'status': 'successful',
  'created': '2025-03-14T23:45:27.885965',
  'started': '2025-03-14T23:45:34.050973',
  'finished': '2025-03-14T23:45:35.915181',
  'updated': '2025-03-14T23:45:35.915181',
  'links': [{'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/993a2b8f-b885-40c3-9d70-56d9273f199f',
    'rel': 'monitor',
    'type': 'application/json',
    'title': 'job status info'},
   {'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/993a2b8f-b885-40c3-9d70-56d9273f199f/results',
    'rel': 'results'}],
  'metadata': {'results': {'asset': {'value': {'type': 'application/netcdf',
      'href': 'https://object-store.os-api.cci2.ecmwf.int:443/cci2-prod-cache-1/2025-03-14/d354f0e372f931bb7965a3c189a4d67d.nc',
      'file:checksum': '48805b3be456283f04ee960c17b1497f',
      'file:size': 201905,
      'file:local_path': 's3://cci2-prod-cache-1/2025-03-14/d354f0e372f931bb7965a3c189a4d67d.nc'}}},
   'datasetMetadata': {'title': 'ERA5-Land hourly data from 1950 to present'},
   'qos': {'status': {}},
   'origin': 'api'}},
  {'processID': 'reanalysis-era5-land',
   'type': 'process',
   'jobID': '8ec42bbf-7394-4f56-babc-6b9e0e5d3036',
   'status': 'failed',
   'created': '2025-03-06T01:49:44.010872',
   'finished': '2025-03-06T01:49:48.884921',
   'updated': '2025-03-06T01:49:48.879550',
   'links': [{'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/8ec42bbf-7394-4f56-babc-6b9e0e5d3036',
     'rel': 'monitor',
     'type': 'application/json',
     'title': 'job status info'},
    {'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs/8ec42bbf-7394-4f56-babc-6b9e0e5d3036/results',
     'rel': 'results'}],
   'metadata': {'results': {'type': 'job results failed',
     'title': 'The job has failed.',
     'status': 400,
     'trace_id': '404ddccb-46e0-445f-9a5f-1a7eb27f287e',
     'traceback': 'Number of API queued requests for this dataset is temporarily limited. Please configure your scripts accordingly '},
    'datasetMetadata': {'title': 'ERA5-Land hourly data from 1950 to present'},
    'qos': {'status': {}},
    'origin': 'api'}}],
 'links': [{'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs?limit=100',
   'rel': 'self',
   'title': 'list of submitted jobs'},
  {'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs?limit=100&cursor=MjAyNS0wMy0wNiAwMTo0OTo0NC4wMTA4NzI%3D&back=False',
   'rel': 'next'},
  {'href': 'https://cds.climate.copernicus.eu/api/retrieve/v1/jobs?limit=100&cursor=MjAyNS0wMy0xNCAyMzo0NToyNy44ODU5NjU%3D&back=True',
   'rel': 'prev'}]
```
### Download the requested file
If a GET request is made to a single job, then a GET request needs to be made to the 'results' link (contained within the job status response) to find the download link. 

This results url link looks like:
```
job_results_url = '{base_url}/retrieve/v1/jobs/{job_id}/results'
```
And a GET request using the python requests package would be the following:
```
resp = requests.get(job_results_url, headers=headers)
```
The header response should look like this:
```
results_response = {
   'asset': {'value': {'type': 'application/netcdf',
   'href': 'https://object-store.os-api.cci2.ecmwf.int:443/cci2-prod-cache-1/2025-03-14/d354f0e372f931bb7965a3c189a4d67d.nc',
   'file:checksum': '48805b3be456283f04ee960c17b1497f',
   'file:size': 201905,
   'file:local_path': 's3://cci2-prod-cache-1/2025-03-14/d354f0e372f931bb7965a3c189a4d67d.nc'}}
   }
```
You'll want the 'href' link to download the file. Downloading the file does not require the private user key. It's a public link.

This download link can be obtained directly from the response when using the GET request to get the statuses of all jobs.

### Limits
The ECMWF sets limits on various aspects of the API and they change them as they see fit without feedback to the user. The main issue I've run into is the number of jobs that can be queued at a time. In the past this was 32, but seems to have gone down to ~20. I've seen it as low as 10 at various times. If you don't want to bother with handling these kinds of limits, then set the number of jobs that you queue to 10-15.






