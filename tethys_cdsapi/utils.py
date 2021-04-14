#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 08:59:16 2021

@author: mike
"""







##########################################3
### Objects

ds_cols = ['feature', 'parameter', 'frequency_interval', 'aggregation_statistic', 'units', 'wrf_standard_name', 'cf_standard_name', 'scale_factor']

file_naming = '{param}_{from_year}-{to_year}_{product}.nc'

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

param_file_mappings = {'temp_at_2': ['2m_temperature_*.nc'],
                       'precip_at_0': ['total_precipitation_*.nc'],
                       'snow_at_0': ['snowfall_*.nc'],
                       'runoff_at_0': ['surface_runoff_*.nc'],
                       'recharge_at_0': ['sub_surface_runoff_*.nc'],
                       'pressure_at_0': ['surface_pressure_*.nc'],
                       'shortwave_rad_at_0': ['surface_net_solar_radiation_*.nc'],
                       'longwave_rad_at_0': ['surface_net_thermal_radiation_*.nc'],
                       'heat_flux_at_0': ['surface_latent_heat_flux_*.nc'],
                       'relative_humidity_at_2': ['2m_temperature_*.nc', '2m_dewpoint_temperature_*.nc'],
                       'wind_speed_at_2': ['10m_u_component_of_wind_*.nc', '10m_v_component_of_wind_*.nc'],
                       'reference_et_at_0': ['2m_temperature_*.nc', '2m_dewpoint_temperature_*.nc', '10m_u_component_of_wind_*.nc', '10m_v_component_of_wind_*.nc', 'surface_net_solar_radiation_*.nc', 'surface_net_thermal_radiation_*.nc', 'surface_latent_heat_flux_*.nc'],
                       'pet_at_0': ['potential_evaporation_*.nc'],
                       'evaporation_at_0': ['total_evaporation_*.nc']
                       }






