#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 09:00:01 2021

@author: mike
"""
import os
import xarray as xr
# import cdsapi

######################################
### Parameters

save_path = '/media/sdb1/Data/ecmwf/era5-land'

parameter = '2m_temperature'
dem = 'geo_1279l4_0.1x0.1.grib2_v4_unpack.nc'

lat = -43.4
lon = 170.5

# altitude = geopotential / 9.80665

####################################
###

nc_path = os.path.join(save_path, parameter + '_*.nc')
ds2 = xr.open_mfdataset(nc_path)


ds1 = xr.open_dataset('download.nc')
ds2 = xr.open_dataset('download2.nc')

ds3 = ds2.t2m.sel(time=slice('1991-01-02', '1991-01-02'))

ds3.plot(x='longitude', y='latitude', col='time', col_wrap=3)


dem1 = xr.open_dataset(os.path.join(save_path, dem))
dem2 = dem1.squeeze('time')

dem3 = dem2.sel(longitude=lon, latitude=lat)
































