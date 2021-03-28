#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 09:00:01 2021

@author: mike
"""
import xarray as xr
import cdsapi

c = cdsapi.Client()

# c.retrieve(
#     'reanalysis-era5-land',
#     {
#         'format': 'netcdf',
#         'variable': '2m_temperature',
#         'year': '2020',
#         'month': [
#             '01', '02', '03',
#             '04', '05', '06',
#             '07', '08', '09',
#             '10', '11', '12',
#         ],
#         'day': [
#             '01', '02', '03',
#             '04', '05', '06',
#             '07', '08', '09',
#             '10', '11', '12',
#             '13', '14', '15',
#             '16', '17', '18',
#             '19', '20', '21',
#             '22', '23', '24',
#             '25', '26', '27',
#             '28', '29', '30',
#             '31',
#         ],
#         'time': [
#             '00:00', '01:00', '02:00',
#             '03:00', '04:00', '05:00',
#             '06:00', '07:00', '08:00',
#             '09:00', '10:00', '11:00',
#             '12:00', '13:00', '14:00',
#             '15:00', '16:00', '17:00',
#             '18:00', '19:00', '20:00',
#             '21:00', '22:00', '23:00',
#         ],
#         'area': [
#             -40.4, 166.1, -47.3,
#             174.5,
#         ],
#     },
#     'download.nc')

# c.retrieve(
#     'reanalysis-era5-land',
#     {
#         'format': 'netcdf',
#         'variable': [
#             '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
#             '2m_temperature', 'snow_depth_water_equivalent',
#             'sub_surface_runoff', 'surface_latent_heat_flux', 'surface_net_solar_radiation',
#             'surface_pressure', 'surface_runoff', 'total_precipitation',
#         ],
#         'year': '2020',
#         'month': '01',
#         'day': [
#             '01', '02', '03',
#             '04', '05', '06',
#             '07', '08', '09',
#             '10', '11', '12',
#             '13', '14', '15',
#             '16', '17', '18',
#             '19', '20', '21',
#             '22', '23', '24',
#             '25', '26', '27',
#             '28', '29', '30',
#             '31',
#         ],
#         'time': [
#             '00:00', '01:00', '02:00',
#             '03:00', '04:00', '05:00',
#             '06:00', '07:00', '08:00',
#             '09:00', '10:00', '11:00',
#             '12:00', '13:00', '14:00',
#             '15:00', '16:00', '17:00',
#             '18:00', '19:00', '20:00',
#             '21:00', '22:00', '23:00',
#         ],
#         'area': [
#             -40.4, 166.1, -47.3,
#             174.5,
#         ],
#     },
#     'download2.nc')





ds1 = xr.open_dataset('download.nc')
ds2 = xr.open_dataset('download2.nc')

ds3 = ds2.t2m.sel(time=slice('1991-01-02', '1991-01-02'))

ds3.plot(x='longitude', y='latitude', col='time', col_wrap=3)





































