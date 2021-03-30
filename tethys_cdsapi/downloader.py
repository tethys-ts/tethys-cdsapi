#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 10:32:40 2021

@author: mike
"""
import os
import xarray as xr
import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool, Pool
import cdsapi
import requests
from tethys_cdsapi.utils import file_naming, available_parameters
import urllib3
urllib3.disable_warnings()

##############################################
### Parameters

months = ['01', '02', '03',
         '04', '05', '06',
         '07', '08', '09',
         '10', '11', '12']

days = ['01', '02', '03',
       '04', '05', '06',
       '07', '08', '09',
       '10', '11', '12',
       '13', '14', '15',
       '16', '17', '18',
       '19', '20', '21',
       '22', '23', '24',
       '25', '26', '27',
       '28', '29', '30',
       '31']

times = ['00:00', '01:00', '02:00',
        '03:00', '04:00', '05:00',
        '06:00', '07:00', '08:00',
        '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00',
        '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00',
        '21:00', '22:00', '23:00']


########################################
### Main classes


class Downloader(object):
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
    def __init__(self, url, key, save_path):
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
        if isinstance(save_path, str):
            setattr(self, 'save_path', save_path)
        else:
            raise TypeError('save_path must be a str.')

        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        sess.mount('https://', adapter)

        client = cdsapi.Client(url=url, key=key, session=sess)

        setattr(self, 'client', client)
        setattr(self, 'available_parameters', available_parameters)


    def download(self, product, parameters, from_date, to_date, bbox):
        """
        The method to do the actual downloading of the files. The current maximum queue limit is 35 requests per user and this has been set as the number of threads to use. The cdsapi blocks the threads until they finished downloading. This can take a very long time for many large files...make sure this process can run happily without interruption for a while...

        This method currently has a minimum download period of one year.

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

        if isinstance(from_date, (str, pd.Timestamp)):
            from_date1 = pd.Timestamp(from_date) - pd.offsets.MonthBegin(0)
        else:
            raise TypeError('from_date must be either str or Timestamp.')
        if isinstance(to_date, (str, pd.Timestamp)):
            to_date1 = pd.Timestamp(to_date) + pd.offsets.MonthEnd(0)
        else:
            raise TypeError('to_date must be either str or Timestamp.')

        if isinstance(bbox, list):
            if len(bbox) != 4:
                raise ValueError('bbox must be a list of 4 floats.')
            else:
                bbox1 = np.round(bbox, 1).tolist()
        else:
            raise TypeError('bbox must be a list of 4 floats.')

        ## Split dates into download chunks
        dates1 = pd.date_range(from_date1, to_date1, freq='11Y')
        dates_min = (dates1 - pd.offsets.YearBegin(1)).year.tolist()
        dates_max = (dates1[1:] - pd.offsets.YearEnd(1)).year.tolist()
        dates_max = dates_max + [to_date1.year]
        dates_combo = list(zip(dates_min, dates_max))
        # dates2 = pd.DataFrame(1, index=dates1, columns=['val'])
        # dates2.index.name = 'year'

        # lats = np.arange(bbox1[2], bbox1[0]+0.1, step=0.1)
        # lons = np.arange(bbox1[1], bbox1[3]+0.1, step=0.1)

        # base_len = len(lats) * len(lons)

        # dates2a = dates2.resample('Y').sum()
        # dates3 = (dates2a.cumsum()//100000).reset_index()
        # dates_grp = dates3.groupby('val')['year']
        # dates_min = (dates_grp.min() - pd.offsets.YearBegin(1)).dt.year
        # dates_min.name = 'from_year'
        # dates_max = dates_grp.max().dt.year
        # dates_max.name = 'to_year'
        # dates4 = pd.concat([dates_min, dates_max], axis=1)

        ## Create requests
        req_list = []
        for p in params:
            for d in dates_combo:
                year1 = d[0]
                year2 = d[1]
                years = [str(y) for y in list(range(year1, year2+1))]
                dict1 = {'format': 'netcdf', 'variable': p, 'year': years, 'month': months, 'day': days, 'time': times, 'area': bbox1}
                file_name = file_naming.format(param=p, from_year=year1, to_year=year2, product=product)
                file_path = os.path.join(self.save_path, file_name)

                req_list.append((product, dict1, file_path))

        ## Run requests
        with ThreadPool(32) as pool:
            output = pool.starmap(self.client.retrieve, req_list)
            pool.close()
            pool.join()

        print('Finished')






