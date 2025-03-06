#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 18 10:32:40 2021

@author: mike
"""
import pathlib
import os
import pandas as pd
import numpy as np
from multiprocessing.pool import ThreadPool, Pool
import concurrent.futures
import cdsapi
import requests
from time import sleep
import copy
import urllib3
urllib3.disable_warnings()

##############################################
### Parameters

# y_ints = ['{:d}Y'.format(y) for y in np.arange(1, 12)]

available_freq_intervals = ['1D', 'D', '1M', 'M', 'Y']
# available_freq_intervals.extend(y_ints)

hours = ['00:00', '01:00', '02:00',
        '03:00', '04:00', '05:00',
        '06:00', '07:00', '08:00',
        '09:00', '10:00', '11:00',
        '12:00', '13:00', '14:00',
        '15:00', '16:00', '17:00',
        '18:00', '19:00', '20:00',
        '21:00', '22:00', '23:00']

file_naming = '{param}.{from_date}-{to_date}.{product}.{ext}'

ext_dict = {'netcdf': 'nc', 'grib': 'grib'}

available_variables = {'reanalysis-era5-land': [
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
        ],
    'reanalysis-era5-pressure-levels': [
            'divergence', 'fraction_of_cloud_cover', 'geopotential',
            'ozone_mass_mixing_ratio', 'potential_vorticity', 'relative_humidity',
            'specific_cloud_ice_water_content', 'specific_cloud_liquid_water_content', 'specific_humidity',
            'specific_rain_water_content', 'specific_snow_water_content', 'temperature',
            'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity',
            'vorticity',
        ],
    'reanalysis-era5-single-levels': [
            '100m_u_component_of_wind', '100m_v_component_of_wind', '10m_u_component_of_neutral_wind',
            '10m_u_component_of_wind', '10m_v_component_of_neutral_wind', '10m_v_component_of_wind',
            '10m_wind_gust_since_previous_post_processing', '2m_dewpoint_temperature', '2m_temperature',
            'air_density_over_the_oceans', 'angle_of_sub_gridscale_orography', 'anisotropy_of_sub_gridscale_orography',
            'benjamin_feir_index', 'boundary_layer_dissipation', 'boundary_layer_height',
            'charnock', 'clear_sky_direct_solar_radiation_at_surface', 'cloud_base_height',
            'coefficient_of_drag_with_waves', 'convective_available_potential_energy', 'convective_inhibition',
            'convective_precipitation', 'convective_rain_rate', 'convective_snowfall',
            'convective_snowfall_rate_water_equivalent', 'downward_uv_radiation_at_the_surface', 'duct_base_height',
            'eastward_gravity_wave_surface_stress', 'eastward_turbulent_surface_stress', 'evaporation',
            'forecast_albedo', 'forecast_logarithm_of_surface_roughness_for_heat', 'forecast_surface_roughness',
            'free_convective_velocity_over_the_oceans', 'friction_velocity', 'geopotential',
            'gravity_wave_dissipation', 'high_cloud_cover', 'high_vegetation_cover',
            'ice_temperature_layer_1', 'ice_temperature_layer_2', 'ice_temperature_layer_3',
            'ice_temperature_layer_4', 'instantaneous_10m_wind_gust', 'instantaneous_eastward_turbulent_surface_stress',
            'instantaneous_large_scale_surface_precipitation_fraction', 'instantaneous_moisture_flux', 'instantaneous_northward_turbulent_surface_stress',
            'instantaneous_surface_sensible_heat_flux', 'k_index', 'lake_bottom_temperature',
            'lake_cover', 'lake_depth', 'lake_ice_depth',
            'lake_ice_temperature', 'lake_mix_layer_depth', 'lake_mix_layer_temperature',
            'lake_shape_factor', 'lake_total_layer_temperature', 'land_sea_mask',
            'large_scale_precipitation', 'large_scale_precipitation_fraction', 'large_scale_rain_rate',
            'large_scale_snowfall', 'large_scale_snowfall_rate_water_equivalent', 'leaf_area_index_high_vegetation',
            'leaf_area_index_low_vegetation', 'low_cloud_cover', 'low_vegetation_cover',
            'maximum_2m_temperature_since_previous_post_processing', 'maximum_individual_wave_height', 'maximum_total_precipitation_rate_since_previous_post_processing',
            'mean_boundary_layer_dissipation', 'mean_convective_precipitation_rate', 'mean_convective_snowfall_rate',
            'mean_direction_of_total_swell', 'mean_direction_of_wind_waves', 'mean_eastward_gravity_wave_surface_stress',
            'mean_eastward_turbulent_surface_stress', 'mean_evaporation_rate', 'mean_gravity_wave_dissipation',
            'mean_large_scale_precipitation_fraction', 'mean_large_scale_precipitation_rate', 'mean_large_scale_snowfall_rate',
            'mean_northward_gravity_wave_surface_stress', 'mean_northward_turbulent_surface_stress', 'mean_period_of_total_swell',
            'mean_period_of_wind_waves', 'mean_potential_evaporation_rate', 'mean_runoff_rate',
            'mean_sea_level_pressure', 'mean_snow_evaporation_rate', 'mean_snowfall_rate',
            'mean_snowmelt_rate', 'mean_square_slope_of_waves', 'mean_sub_surface_runoff_rate',
            'mean_surface_direct_short_wave_radiation_flux', 'mean_surface_direct_short_wave_radiation_flux_clear_sky', 'mean_surface_downward_long_wave_radiation_flux',
            'mean_surface_downward_long_wave_radiation_flux_clear_sky', 'mean_surface_downward_short_wave_radiation_flux', 'mean_surface_downward_short_wave_radiation_flux_clear_sky',
            'mean_surface_downward_uv_radiation_flux', 'mean_surface_latent_heat_flux', 'mean_surface_net_long_wave_radiation_flux',
            'mean_surface_net_long_wave_radiation_flux_clear_sky', 'mean_surface_net_short_wave_radiation_flux', 'mean_surface_net_short_wave_radiation_flux_clear_sky',
            'mean_surface_runoff_rate', 'mean_surface_sensible_heat_flux', 'mean_top_downward_short_wave_radiation_flux',
            'mean_top_net_long_wave_radiation_flux', 'mean_top_net_long_wave_radiation_flux_clear_sky', 'mean_top_net_short_wave_radiation_flux',
            'mean_top_net_short_wave_radiation_flux_clear_sky', 'mean_total_precipitation_rate', 'mean_vertical_gradient_of_refractivity_inside_trapping_layer',
            'mean_vertically_integrated_moisture_divergence', 'mean_wave_direction', 'mean_wave_direction_of_first_swell_partition',
            'mean_wave_direction_of_second_swell_partition', 'mean_wave_direction_of_third_swell_partition', 'mean_wave_period',
            'mean_wave_period_based_on_first_moment', 'mean_wave_period_based_on_first_moment_for_swell', 'mean_wave_period_based_on_first_moment_for_wind_waves',
            'mean_wave_period_based_on_second_moment_for_swell', 'mean_wave_period_based_on_second_moment_for_wind_waves', 'mean_wave_period_of_first_swell_partition',
            'mean_wave_period_of_second_swell_partition', 'mean_wave_period_of_third_swell_partition', 'mean_zero_crossing_wave_period',
            'medium_cloud_cover', 'minimum_2m_temperature_since_previous_post_processing', 'minimum_total_precipitation_rate_since_previous_post_processing',
            'minimum_vertical_gradient_of_refractivity_inside_trapping_layer', 'model_bathymetry', 'near_ir_albedo_for_diffuse_radiation',
            'near_ir_albedo_for_direct_radiation', 'normalized_energy_flux_into_ocean', 'normalized_energy_flux_into_waves',
            'normalized_stress_into_ocean', 'northward_gravity_wave_surface_stress', 'northward_turbulent_surface_stress',
            'ocean_surface_stress_equivalent_10m_neutral_wind_direction', 'ocean_surface_stress_equivalent_10m_neutral_wind_speed', 'peak_wave_period',
            'period_corresponding_to_maximum_individual_wave_height', 'potential_evaporation', 'precipitation_type',
            'runoff', 'sea_ice_cover', 'sea_surface_temperature',
            'significant_height_of_combined_wind_waves_and_swell', 'significant_height_of_total_swell', 'significant_height_of_wind_waves',
            'significant_wave_height_of_first_swell_partition', 'significant_wave_height_of_second_swell_partition', 'significant_wave_height_of_third_swell_partition',
            'skin_reservoir_content', 'skin_temperature', 'slope_of_sub_gridscale_orography',
            'snow_albedo', 'snow_density', 'snow_depth',
            'snow_evaporation', 'snowfall', 'snowmelt',
            'soil_temperature_level_1', 'soil_temperature_level_2', 'soil_temperature_level_3',
            'soil_temperature_level_4', 'soil_type', 'standard_deviation_of_filtered_subgrid_orography',
            'standard_deviation_of_orography', 'sub_surface_runoff', 'surface_latent_heat_flux',
            'surface_net_solar_radiation', 'surface_net_solar_radiation_clear_sky', 'surface_net_thermal_radiation',
            'surface_net_thermal_radiation_clear_sky', 'surface_pressure', 'surface_runoff',
            'surface_sensible_heat_flux', 'surface_solar_radiation_downward_clear_sky', 'surface_solar_radiation_downwards',
            'surface_thermal_radiation_downward_clear_sky', 'surface_thermal_radiation_downwards', 'temperature_of_snow_layer',
            'toa_incident_solar_radiation', 'top_net_solar_radiation', 'top_net_solar_radiation_clear_sky',
            'top_net_thermal_radiation', 'top_net_thermal_radiation_clear_sky', 'total_cloud_cover',
            'total_column_cloud_ice_water', 'total_column_cloud_liquid_water', 'total_column_ozone',
            'total_column_rain_water', 'total_column_snow_water', 'total_column_supercooled_liquid_water',
            'total_column_water', 'total_column_water_vapour', 'total_precipitation',
            'total_sky_direct_solar_radiation_at_surface', 'total_totals_index', 'trapping_layer_base_height',
            'trapping_layer_top_height', 'type_of_high_vegetation', 'type_of_low_vegetation',
            'u_component_stokes_drift', 'uv_visible_albedo_for_diffuse_radiation', 'uv_visible_albedo_for_direct_radiation',
            'v_component_stokes_drift', 'vertical_integral_of_divergence_of_cloud_frozen_water_flux', 'vertical_integral_of_divergence_of_cloud_liquid_water_flux',
            'vertical_integral_of_divergence_of_geopotential_flux', 'vertical_integral_of_divergence_of_kinetic_energy_flux', 'vertical_integral_of_divergence_of_mass_flux',
            'vertical_integral_of_divergence_of_moisture_flux', 'vertical_integral_of_divergence_of_ozone_flux', 'vertical_integral_of_divergence_of_thermal_energy_flux',
            'vertical_integral_of_divergence_of_total_energy_flux', 'vertical_integral_of_eastward_cloud_frozen_water_flux', 'vertical_integral_of_eastward_cloud_liquid_water_flux',
            'vertical_integral_of_eastward_geopotential_flux', 'vertical_integral_of_eastward_heat_flux', 'vertical_integral_of_eastward_kinetic_energy_flux',
            'vertical_integral_of_eastward_mass_flux', 'vertical_integral_of_eastward_ozone_flux', 'vertical_integral_of_eastward_total_energy_flux',
            'vertical_integral_of_eastward_water_vapour_flux', 'vertical_integral_of_energy_conversion', 'vertical_integral_of_kinetic_energy',
            'vertical_integral_of_mass_of_atmosphere', 'vertical_integral_of_mass_tendency', 'vertical_integral_of_northward_cloud_frozen_water_flux',
            'vertical_integral_of_northward_cloud_liquid_water_flux', 'vertical_integral_of_northward_geopotential_flux', 'vertical_integral_of_northward_heat_flux',
            'vertical_integral_of_northward_kinetic_energy_flux', 'vertical_integral_of_northward_mass_flux', 'vertical_integral_of_northward_ozone_flux',
            'vertical_integral_of_northward_total_energy_flux', 'vertical_integral_of_northward_water_vapour_flux', 'vertical_integral_of_potential_and_internal_energy',
            'vertical_integral_of_potential_internal_and_latent_energy', 'vertical_integral_of_temperature', 'vertical_integral_of_thermal_energy',
            'vertical_integral_of_total_energy', 'vertically_integrated_moisture_divergence', 'volumetric_soil_water_layer_1',
            'volumetric_soil_water_layer_2', 'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4',
            'wave_spectral_directional_width', 'wave_spectral_directional_width_for_swell', 'wave_spectral_directional_width_for_wind_waves',
            'wave_spectral_kurtosis', 'wave_spectral_peakedness', 'wave_spectral_skewness',
            'zero_degree_level',
        ]}

pressure_levels = {'reanalysis-era5-pressure-levels':  [
                1, 2, 3,
                5, 7, 10,
                20, 30, 50,
                70, 100, 125,
                150, 175, 200,
                225, 250, 300,
                350, 400, 450,
                500, 550, 600,
                650, 700, 750,
                775, 800, 825,
                850, 875, 900,
                925, 950, 975,
                1000,
            ]
    }

product_types = {'reanalysis-era5-single-levels': [
            'ensemble_mean', 'ensemble_members', 'ensemble_spread',
            'reanalysis'],
    'reanalysis-era5-pressure-levels': [
                'ensemble_mean', 'ensemble_members', 'ensemble_spread',
                'reanalysis']
    }

### WRF parameters

era5_wrf_products = {
    'reanalysis-era5-pressure-levels':
        [
            'geopotential',
            'relative_humidity',
            'temperature',
            'u_component_of_wind',
            'v_component_of_wind'
            ],
    'reanalysis-era5-single-levels':
        [
            '10m_u_component_of_wind',
            '10m_v_component_of_wind',
            '2m_dewpoint_temperature',
            '2m_temperature',
            'land_sea_mask',
            'mean_sea_level_pressure',
            'sea_ice_cover',
            'sea_surface_temperature',
            'skin_temperature',
            'snow_depth',
            'snow_density',
            'soil_temperature_level_1',
            'soil_temperature_level_2',
            'soil_temperature_level_3',
            'soil_temperature_level_4',
            'surface_pressure',
            'volumetric_soil_water_layer_1',
            'volumetric_soil_water_layer_2',
            'volumetric_soil_water_layer_3',
            'volumetric_soil_water_layer_4'
            ]
        }

########################################
### Helper functions


def time_request(from_date1, to_date1):
    """

    """
    # from_date1 = from_date1 + pd.DateOffset(days=1)
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
        days1 = np.arange(1, 32)

    days = ['{:02d}'.format(d) for d in days1]
    months = ['{:02d}'.format(m) for m in months1]
    years = ['{:04d}'.format(y) for y in years1]

    return {'year': years, 'month': months, 'day': days, 'time': hours}


def download_file(client, name, request, target):
    """

    """
    # r = client.retrieve(name, request)

    # retries = 5
    # while True:
    #     sleep(60)
    #     # r.update()
    #     reply = r.reply
    #     # r.info("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))
    #     print("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))

    #     if reply["state"] in ("completed", 'successful'):
    #         break
    #     # elif reply["state"] in ("queued", "running"):
    #     #     # r.info("Request ID: %s, sleep: %s", reply["request_id"], sleep)
    #     #     sleep(300)
    #     elif reply["state"] in ("failed",):
    #         r.error("Message: %s", reply["error"].get("message"))
    #         r.error("Reason:  %s", reply["error"].get("reason"))

    #         print('Request failed with message: {msg}; and reason: {reason}'.format(msg=reply["error"].get("message"), reason=reply["error"].get("reason")))

    #         ## Remove request
    #         r.delete()

    #         if retries > 0:
    #             ## try again
    #             sleep(60)
    #             r = client.retrieve(name, request)
    #         else:
    #             raise Exception('Request failed with message: {msg}; and reason: {reason}'.format(msg=reply["error"].get("message"), reason=reply["error"].get("reason")))

    # r.download(target)

    client.retrieve(name, request, target)

    return target


class Downloader:
    """

    """
    def __init__(self, client, product, request, target):
        """

        """
        self.client = client
        self.product = product
        self.request = request
        self.target = target


    def download(self):
        """

        """
        # target = download_file(self.client, self.product, self.request, self.target)
        self.client.retrieve(self.product, self.request, self.target)

        return self.target



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
        save_path : str or pathlib.Path
            The path to save the downloaded files.
        threads : int
            The number of simultaneous download/queued requests. Only one request will be processed at one time, but a user can queue many requests. It's unclear if there is a limit to the number of queued requests per user.

        Returns
        -------
        Downloader object
        """
        if isinstance(save_path, str):
            setattr(self, 'save_path', save_path)
        elif isinstance(save_path, pathlib.Path):
            setattr(self, 'save_path', str(save_path))
        else:
            raise TypeError('save_path must be a str or a pathlib.Path.')

        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=threads, pool_maxsize=threads)
        sess.mount('https://', adapter)

        client = cdsapi.Client(url=url, key=key, session=sess)

        setattr(self, 'client', client)
        setattr(self, 'available_variables', available_variables)
        setattr(self, 'available_products', list(available_variables.keys()))
        setattr(self, 'available_freq_intervals', available_freq_intervals)
        setattr(self, 'available_product_types', product_types)
        setattr(self, 'available_pressure_levels', pressure_levels)
        setattr(self, 'threads', threads)


    def _processing(self, product: str, variables, from_date, to_date, bbox, freq_interval='Y', product_types=None, pressure_levels=None, output_format='netcdf'):
        """

        """
        if product not in self.available_variables.keys():
            raise ValueError('product is not available.')

        # Parameters/variables
        if isinstance(variables, str):
            params = [variables]
        elif isinstance(variables, list):
            params = variables.copy()
        else:
            raise TypeError('variables must be a str or a list of str.')

        for p in params:
            av = self.available_variables[product]
            if not p in av:
                raise ValueError(p + ' is not one of the available variables for this product.')

        # freq intervals
        if not freq_interval in self.available_freq_intervals:
            raise ValueError('freq_interval must be one of: ' + str(self.available_freq_intervals))

        # Product types
        if product in self.available_product_types:
            if isinstance(product_types, str):
                product_types1 = [product_types]
            elif not isinstance(product_types, list):
                raise TypeError('The requested product has required product_types, but none have been specified.')
            pt_bool = all([p in self.available_product_types[product] for p in product_types1])

            if not pt_bool:
                raise ValueError('Not all requested product_types are in the available_product_types.')
        else:
            product_types1 = None

        # Pressure levels
        if product in self.available_pressure_levels:
            if isinstance(pressure_levels, list):
                pressure_levels1 = pressure_levels
            elif isinstance(pressure_levels, int):
                pressure_levels1 = [pressure_levels]
            else:
                raise TypeError('The requested product has required pressure_levels, but none have been specified.')
            pl_bool = all([p in self.available_pressure_levels[product] for p in pressure_levels1])

            if not pl_bool:
                raise ValueError('Not all requested pressure_levels are in the available_pressure_levels.')
        else:
            pressure_levels1 = None

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

        ## Formats
        if output_format not in ['netcdf', 'grib']:
            raise ValueError('output_format must be either netcdf or grib')

        ## Split dates into download chunks
        dates1 = pd.date_range(from_date1, to_date1, freq=freq_interval)

        if dates1.empty:
            raise ValueError('The frequency interval is too long for the input time period. Use a shorter frequency interval.')

        # if from_date1 < dates1[0]:
        #     dates1 = pd.DatetimeIndex([from_date1]).append(dates1)
        if to_date1 > dates1[-1]:
            dates1 = dates1.append(pd.DatetimeIndex([to_date1]))

        return params, product_types1, bbox1, pressure_levels1, dates1, from_date1


    def downloader(self, product: str, variables, from_date, to_date, bbox, freq_interval='Y', product_types=None, pressure_levels=None, output_format='netcdf', zipped=False):
        """
        The method to do the actual downloading of the files. The current maximum queue limit is 32 requests per user and this has been set as the number of threads to use. The cdsapi blocks the threads until they finished downloading. This can take a very long time for many large files...make sure this process can run happily without interruption for a while...
        This method does not check to make sure you do not exceede the CDS extraction limit of 120,000 values, so be sure to make your request of a sane size. When in doubt, just reduce the amount per request by lowering the freq_interval.

        The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).
        This extraction resolution is due to the limitations of the cdsapi.

        Parameters
        ----------
        product : str
            The requested product. Look at the available_parameters keys for all options.
        variables : str or list of str
            The requested variables. Look at the available_variables values for all options.
        from_date : str or Timestamp
            The start date of the extraction.
        to_date : str or Timestamp
            The end date of the extraction.
        bbox : list of float
            The bounding box of lat and lon for the requested area. It must be in the order of [upper lat, left lon, lower lat, right lon].
        freq_interval : str
            Pandas frequency string representing the time interval of each request. The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).
        product_types : str or list of str
            Some products have product types and if they do they need to be specified. Check the available_product_types object for the available options.
        pressure_levels : int or list of int
            Some products have pressure levels and if they do they need to be specified. Check the available_pressure_levels object for the available options.
        output_format : str
            The output format for the file. Must be either netcdf or grib.

        Returns
        -------
        Iterator of Downloaders
        """
        ## Get input params
        params, product_types1, bbox1, pressure_levels1, dates1, from_date1 = self._processing(product, variables, from_date, to_date, bbox, freq_interval, product_types, pressure_levels, output_format)

        ## Create requests
        # req_list = []
        for p in params:
            dict1 = {'data_format': output_format, 'variable': p, 'area': bbox1}
            if not zipped:
                dict1['download_format'] = 'unarchived'

            if isinstance(product_types1, list):
                dict1['product_type'] = product_types1

            if isinstance(pressure_levels1, list):
                dict1['pressure_level'] = [str(p) for p in pressure_levels1]

            for i, tdate in enumerate(dates1):
                if i == 0:
                    fdate = from_date1
                else:
                    fdate = dates1[i-1] + pd.DateOffset(days=1)

                dict2 = copy.deepcopy(dict1)

                time_dict = time_request(fdate, tdate)

                dict2.update(time_dict)

                file_name = file_naming.format(param=p, from_date=fdate.strftime('%Y%m%d'), to_date=tdate.strftime('%Y%m%d'), product=product, ext=ext_dict[output_format])
                file_path = os.path.join(self.save_path, file_name)

                # req_list.append({'name': product, 'request': dict2, 'target': file_path})
                yield Downloader(self.client, product, dict2, file_path)

        ## Run requests
        # with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
        #     futures = []
        #     for req in req_list:
        #         f = executor.submit(download_file, self.client, req['name'], req['request'], req['target'])
        #         futures.append(f)

        # runs = concurrent.futures.wait(futures)

        # paths = []
        # for run in runs[0]:
        #     paths.append(run.result())

        # print('Finished')

        # return paths


    def download(self, product: str, variables, from_date, to_date, bbox, freq_interval='Y', product_types=None, pressure_levels=None, output_format='netcdf', zipped=False):
        """
        The method to do the actual downloading of the files. The current maximum queue limit is 32 requests per user and this has been set as the number of threads to use. The cdsapi blocks the threads until they finished downloading. This can take a very long time for many large files...make sure this process can run happily without interruption for a while...
        This method does not check to make sure you do not exceede the CDS extraction limit of 120,000 values, so be sure to make your request of a sane size. When in doubt, just reduce the amount per request by lowering the freq_interval.

        The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).
        This extraction resolution is due to the limitations of the cdsapi.

        Parameters
        ----------
        product : str
            The requested product. Look at the available_parameters keys for all options.
        variables : str or list of str
            The requested variables. Look at the available_variables values for all options.
        from_date : str or Timestamp
            The start date of the extraction.
        to_date : str or Timestamp
            The end date of the extraction.
        bbox : list of float
            The bounding box of lat and lon for the requested area. It must be in the order of [upper lat, left lon, lower lat, right lon].
        freq_interval : str
            Pandas frequency string representing the time interval of each request. The freq_interval can be 1D or D for daily, 1M or M for monthly, or yearly (Y) with up to 11 years (11Y).
        product_types : str or list of str
            Some products have product types and if they do they need to be specified. Check the available_product_types object for the available options.
        pressure_levels : int or list of int
            Some products have pressure levels and if they do they need to be specified. Check the available_pressure_levels object for the available options.
        output_format : str
            The output format for the file. Must be either netcdf or grib.

        Returns
        -------
        Paths as strings
        """
        ## Get input params
        params, product_types1, bbox1, pressure_levels1, dates1, from_date1 = self._processing(product, variables, from_date, to_date, bbox, freq_interval, product_types, pressure_levels, output_format)

        ## Create requests
        req_list = []
        for p in params:
            dict1 = {'data_format': output_format, 'variable': p, 'area': bbox1}
            if not zipped:
                dict1['download_format'] = 'unarchived'

            if isinstance(product_types1, list):
                dict1['product_type'] = product_types1

            if isinstance(pressure_levels1, list):
                dict1['pressure_level'] = [str(p) for p in pressure_levels1]

            for i, tdate in enumerate(dates1):
                if i == 0:
                    fdate = from_date1
                else:
                    fdate = dates1[i-1] + pd.DateOffset(days=1)

                dict2 = copy.deepcopy(dict1)

                time_dict = time_request(fdate, tdate)

                dict2.update(time_dict)

                file_name = file_naming.format(param=p, from_date=fdate.strftime('%Y%m%d'), to_date=tdate.strftime('%Y%m%d'), product=product, ext=ext_dict[output_format])
                file_path = os.path.join(self.save_path, file_name)

                req_list.append({'name': product, 'request': dict2, 'target': file_path})
                # yield Downloader(self.client, product, dict2, file_path)

        ## Run requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = []
            for req in req_list:
                sleep(2)
                f = executor.submit(download_file, self.client, req['name'], req['request'], req['target'])
                futures.append(f)

        runs = concurrent.futures.wait(futures)

        paths = []
        for run in runs[0]:
            paths.append(run.result())

        # print('Finished')

        return paths



