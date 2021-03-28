#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 16:54:02 2021

@author: mike
"""
import os
import yaml
import xarray as xr
from tethys_cdsapi import Downloader

#######################################################
### Testing

base_dir = os.path.realpath(os.path.dirname(__file__))


with open(os.path.join(base_dir, 'parameters.yml')) as param:
    param = yaml.safe_load(param)


source = param['source']

url = source['url_endpoint']
key = source['key']

parameters = '2m_temperature'
from_date = '1981-01-01'
to_date = '2020-12-01'
bbox = [-34.3, 166.3, -47.3, 178.6]
product = 'reanalysis-era5-land'
save_path = '/media/sdb1/Data/ecmwf/era5-land'

ds2 = xr.open_dataset(os.path.join(save_path, '2m_temperature_1981-1991_reanalysis-era5-land.nc'))


# self = Downloader(url, key, save_path)



















