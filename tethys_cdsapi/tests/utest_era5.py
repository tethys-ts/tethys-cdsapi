#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 16:54:02 2021

@author: mike
"""
import os
import yaml
import xarray as xr
# from tethys_cdsapi import Downloader

#######################################################
### Testing

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters.yml')) as param:
    param = yaml.safe_load(param)

source = param['source']

# url = source['url_endpoint']
# key = source['key']

# parameters = '2m_temperature'
# from_date = '1981-01-01'
# to_date = '2020-12-01'
# bbox = [-34.3, 166.3, -47.3, 178.6]
# product = 'reanalysis-era5-land'
owner = 'NZ Open Modelling Consortium'
# product_code = 'NZ South Island 3km v01'
product_code = 'Test 1km v01'
data_license = "https://creativecommons.org/licenses/by/4.0/"
attribution = "Data licensed by the NZ Open Data Consortium"

parameter_code = 'temp_at_2'
save_path = '/media/sdb1/Data/ecmwf/era5-land'
nc_files = '2m_temperature_*'
nc = os.path.join(save_path, nc_files)

geop_file = 'geo_1279l4_0.1x0.1.grib2_v4_unpack.nc'
geopotential_path = os.path.join(save_path, geop_file)

ds2 = xr.open_dataset(os.path.join(save_path, '2m_temperature_1981-1991_reanalysis-era5-land.nc'))
ds2 = xr.open_dataset(os.path.join(save_path, 'example_data1.nc'))

self = Processor(nc, geopotential_path)
ds1 = self.build_dataset(parameter_code, owner, product_code, data_license, attribution)
data1 = self.get_results()
# self = Downloader(url, key, save_path)



















