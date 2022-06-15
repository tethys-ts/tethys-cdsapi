#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 10:32:40 2021

@author: mike
"""
import os
import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures
import cdsapi
import requests
from time import sleep
import urllib3
urllib3.disable_warnings()

##############################################
### Parameters

y_ints = ['{:d}Y'.format(y) for y in np.arange(1, 12)]

available_freq_intervals = ['1D', 'D', '1M', 'M', 'Y']
available_freq_intervals.extend(y_ints)

hours = ['00:00', '01:00', '02:00',
        '03:00', '04:00', '05:00',
        '06:00', '07:00', '08:00',
        '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00',
        '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00',
        '21:00', '22:00', '23:00']

file_naming = '{param}_{from_date}-{to_date}_{product}.nc'

available_parameters = {'reanalysis-era5-land': [
            '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
            '2m_temperature', 'evaporation_from_bare_soil', 'evaporation_from_open_water_surfaces_excluding_oceans',
            'evaporation_from_the_top_of_canopy', 'evaporation_from_vegetation_transpiration', 'forecast_albedo', 'lake_bottom_temperature', 'lake_ice_depth', 'lake_ice_temperature',
            'lake_mix_layer_depth', 'lake_mix_layer_temperature', 'lake_shape_factor',
            'lake_total_layer_temperature', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation', 'potential_evaporation', 'runoff', 'skin_reservoir_content',
            'skin_temperature', 'snow_albedo', 'snow_cover',
            'snow_density', 'snow_depth', 'snow_depth_water_equivalent',
            'snow_evaporation', 'snowfall', 'snowmelt',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'soil_temperature_level_4', 'sub_surface_runoff', 'surface_latent_heat_flux',
            'surface_net_solar_radiation', 'surface_net_thermal_radiation', 'surface_pressure',
            'surface_runoff', 'surface_sensible_heat_flux', 'surface_solar_radiation_downwards',
            'surface_thermal_radiation_downwards', 'temperature_of_snow_layer', 'total_evaporation',
            'total_precipitation', 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
            'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
        ]}

########################################
### Helper functions


def time_request(from_date1, to_date1):
    """

    """
    from_date1 = from_date1 + pd.DateOffset(days=1)
    from_day = from_date1.day
    to_day = to_date1.day
    from_month = from_date1.month
    to_month = to_date1.month
    from_year = from_date1.year
    to_year = to_date1.year

    if from_year == to_year:
        years1 = [from_year]
        months1 = np.arange(from_month, to_month+1)

        if from_month == to_month:
            days1 = np.arange(from_day, to_day+1)
        else:
            days1 = np.arange(1, 32)
    else:
        years1 = np.arange(from_year, to_year+1)
        months1 = np.arange(1, 13)

    days = ['{:02d}'.format(d) for d in days1]
    months = ['{:02d}'.format(m) for m in months1]
    years = ['{:04d}'.format(y) for y in years1]

    return {'year': years, 'month': months, 'day': days, 'time': hours}


def download_file(client, name, request, target):
    """

    """
    r = client.retrieve(name, request)

    retries = 5
    while True:
        sleep(300)
        r.update()
        reply = r.reply
        # r.info("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))
        # print("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))

        if reply["state"] == "completed":
            break
        # elif reply["state"] in ("queued", "running"):
        #     # r.info("Request ID: %s, sleep: %s", reply["request_id"], sleep)
        #     sleep(300)
        elif reply["state"] in ("failed",):
            r.error("Message: %s", reply["error"].get("message"))
            r.error("Reason:  %s", reply["error"].get("reason"))

            print('Request failed with message: {msg}; and reason: {reason}'.format(msg=reply["error"].get("message"), reason=reply["error"].get("reason")))

            ## Remove request
            r.delete()

            if retries > 0:
                ## try again
                sleep(60)
                r = client.retrieve(name, request)
            else:
                raise Exception('Request failed with message: {msg}; and reason: {reason}'.format(msg=reply["error"].get("message"), reason=reply["error"].get("reason")))

    r.download(target)

    return target


########################################
### Main class


class CDS(object):
    """
    Class to download CDS data via their cdsapi. This is just a wrapper on top of cdsapi that makes it more useful as an API. The user needs to register by following the procedure here: https://cds.climate.copernicus.eu/api-how-to.

    Parameters
    ----------
    url : str
        The endpoint URL provided after registration.
    key: str
        The key provided after registration.
    save_path : str
        The path to save the downloaded files.

    Returns
    -------
    Downloader object
    """
    ## Initialization
    def __init__(self, url, key, save_path, threads=32):
        """
        Class to download CDS data via their cdsapi. This is just a wrapper on top of cdsapi that makes it more useful as an API. The user needs to register by following the procedure here: https://cds.climate.copernicus.eu/api-how-to.

        Parameters
        ----------
        url : str
            The endpoint URL provided after registration.
        key: str
            The key provided after registration.
        save_path : str
            The path to save the downloaded files.
        threads : int
            The number of simultaneous download/queued requests. Only one request will be processed at one time, but a user can queue many requests. It's unclear if there is a limit to the number of queued requests per user.

        Returns
        -------
        Downloader object
        """
        if isinstance(save_path, str):
            setattr(self, 'save_path', save_path)
        else:
            raise TypeError('save_path must be a str.')

        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=threads, pool_maxsize=threads)
        sess.mount('https://', adapter)

        client = cdsapi.Client(url=url, key=key, session=sess)

        setattr(self, 'client', client)
        setattr(self, 'available_parameters', available_parameters)
        setattr(self, 'available_products', list(available_parameters.keys()))
        setattr(self, 'available_freq_intervals', available_freq_intervals)
        setattr(self, 'threads', threads)


    def download(self, product, parameters, from_date, to_date, bbox, freq_interval='10Y'):
        """
        The method to do the actual downloading of the files. The current maximum queue limit is 32 requests per user and this has been set as the number of threads to use. The cdsapi blocks the threads until they finished downloading. This can take a very long time for many large files...make sure this process can run happily without interruption for a while...

        The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).
        This extraction resolution is due to the limitations of the cdsapi.

        Parameters
        ----------
        product : str
            The requested product. Look at the available_parameters keys for all options.
        parameters : str or list of str
            The requested parameters. Look at the available_parameters values for all options.
        from_date : str or Timestamp
            The start date of the extraction.
        to_date : str or Timestamp
            The end date of the extraction.
        bbox : list of float
            The bounding box of lat and lon for the requested area. It must be in the order of [upper lat, left lon, lower lat, right lon].
        freq_interval : str
            Pandas frequency string representing the time interval of each request. The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).

        Returns
        -------
        None
        """
        ## Run checks
        if product not in available_parameters.keys():
            raise ValueError('product is not available.')

        if isinstance(parameters, str):
            params = [parameters]
        elif isinstance(parameters, list):
            params = parameters.copy()
        else:
            raise TypeError('parameters must be a str or a list of str.')

        for p in params:
            av = self.available_parameters[product]
            if not p in av:
                raise ValueError(p + ' is not one of the available parameters for this product.')

        if not freq_interval in self.available_freq_intervals:
            raise ValueError('freq_interval must be one of: ' + str(self.available_freq_intervals))

        ## Parse dates
        if isinstance(from_date, (str, pd.Timestamp)):
            from_date1 = pd.Timestamp(from_date).floor('D')
        else:
            raise TypeError('from_date must be either str or Timestamp.')
        if isinstance(to_date, (str, pd.Timestamp)):
            to_date1 = pd.Timestamp(to_date).floor('D')
        else:
            raise TypeError('to_date must be either str or Timestamp.')

        ## Parse bbox
        if isinstance(bbox, list):
            if len(bbox) != 4:
                raise ValueError('bbox must be a list of 4 floats.')
            else:
                bbox1 = np.round(bbox, 1).tolist()
        else:
            raise TypeError('bbox must be a list of 4 floats.')

        ## Split dates into download chunks
        dates1 = pd.date_range(from_date1, to_date1, freq=freq_interval)
        if from_date1 < dates1[0]:
            dates1 = pd.DatetimeIndex([from_date1]).append(dates1)
        if to_date1 > dates1[-1]:
            dates1 = dates1.append(pd.DatetimeIndex([to_date1]))

        ## Create requests
        req_list = []
        for p in params:
            dict1 = {'format': 'netcdf', 'variable': p, 'area': bbox1}

            for i, tdate in enumerate(dates1[1:]):
                fdate = dates1[i]
                time_dict = time_request(fdate, tdate)

                dict1.update(time_dict)

                file_name = file_naming.format(param=p, from_date=fdate.strftime('%Y%m%d'), to_date=tdate.strftime('%Y%m%d'), product=product)
                file_path = os.path.join(self.save_path, file_name)

                req_list.append({'name': product, 'request': dict1, 'target': file_path})

        ## Run requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = []
            for req in req_list:
                f = executor.submit(download_file, self.client, req['name'], req['request'], req['target'])
                futures.append(f)

        runs = concurrent.futures.wait(futures)

        paths = []
        for run in runs[0]:
            paths.append(run.result())

        print('Finished')

        return paths






