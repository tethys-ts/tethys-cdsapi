#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 21:08:51 2021

@author: mike
"""
import xarray as xr
import pandas as pd
import numpy as np


###########################################
### Functions


def calc_rh2(wrf_xr):
    """
    Methods to calc relative humidity at 2 meters from the FAO56 manual. Uses 2m temperature and dew point temperature.

    Parameters
    ----------
    wrf_xr : xr.Dataset
        The complete WRF output dataset with Q2, T2, and PSFC.

    Returns
    -------
    xr.DataArray
    """
    ## Assign variables
    t = wrf_xr['t2m'] - 273.15
    dew = wrf_xr['d2m'] - 273.15

    eo = 0.6108*np.exp((17.27*t)/(t + 237.3))
    ea = 0.6108*np.exp((17.27*dew)/(dew + 237.3))

    rh = (ea/eo) * 100

    rh = xr.where(rh > 100, 100, rh)

    return rh


def calc_wind_speed(wrf_xr, height=2):
    """
    Estimate the mean wind speed at 10 or 2 m from the V and U WRF vectors of wind speed. The 2 m method is according to the FAO 56 paper.

    Parameters
    ----------
    wrf_xr : xr.Dataset
        The complete WRF output dataset with Q2, T2, and PSFC.
    height : int
        The height for the estimate.

    Returns
    -------
    xr.DataArray
    """
    u10 = wrf_xr['u10']
    v10 = wrf_xr['v10']

    ws = np.sqrt(u10**2 + v10**2)

    if height == 2:
        ws = ws*4.87/(np.log(67.8*10 - 5.42))
    elif height != 10:
        raise ValueError('height must be either 10 or 2.')

    return ws


def calc_temp2(wrf_xr, units='degC'):
    """

    """
    t2 = wrf_xr['t2m']

    if units == 'degC':
        t2 = t2 - 273.15
    elif units != 'K':
        raise ValueError('units must be either degC or K.')

    return t2


def calc_surface_pressure(wrf_xr, units='hPa'):
    """

    """
    pres = wrf_xr['sp']

    if units == 'hPa':
        pres = pres * 0.01
    elif units == 'kPa':
        pres = pres * 0.001
    elif units != 'Pa':
        raise ValueError('units must be kPa, hPa, or Pa.')

    return pres


def fix_accum(da):
    """

    """
    ## Convert from accumultion to cumultive
    ds2 = da.diff('time')
    ds3 = xr.where(ds2 < 0, da[1:], ds2)
    ds3['time'] = da['time'][:-1]

    return ds3


def calc_precip0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['tp']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1) * 1000

    return ds2


def calc_snow0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['sf']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1)

    return ds2


def calc_runoff0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['sro']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1) * 1000

    return ds2


def calc_recharge0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['ssro']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1) * 1000

    return ds2


def calc_longwave0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['str']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1)

    return ds2


def calc_shortwave0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['ssr']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1)

    return ds2


def calc_heat_flux0(wrf_xr):
    """

    """
    ## Assign variables
    ds1 = wrf_xr['slhf']

    ## Convert from accumultion to cumultive
    ds2 = fix_accum(ds1)

    return ds2


def calc_eto(wrf_xr):
    """

    """
    ## Assign variables
    dew = wrf_xr['d2m'] - 273.15
    t2 = wrf_xr['t2m'] - 273.15
    pres = wrf_xr['sp']
    gamma = (0.665*10**-3)*pres/1000
    G = calc_heat_flux0(wrf_xr) * 0.0036
    R_n = (calc_shortwave0(wrf_xr) + calc_longwave0(wrf_xr)) * 0.0036
    u10 = wrf_xr['u10']
    v10 = wrf_xr['v10']
    ws2 = np.sqrt(u10**2 + v10**2)*4.87/(np.log(67.8*10 - 5.42))

    # Humidity
    e_mean = 0.6108*np.exp(17.27*t2/(t2+237.3))
    e_a = 0.6108*np.exp((17.27*dew)/(dew + 237.3))

    # Vapor pressure
    delta = 4098*(0.6108*np.exp(17.27*t2/(t2 + 237.3)))/((t2 + 237.3)**2)
    # R_ns = (1 - alb)*R_s

    # Calc ETo
    ETo = (0.408*delta*(R_n - G) + gamma*37/(t2 + 273)*ws2*(e_mean - e_a))/(delta + gamma*(1 + 0.34*ws2))

    return ETo


