#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:03:33 2021

@author: mike
"""
import os
import xarray as xr
import pandas as pd
import numpy as np
from tethys_cdsapi import virtual_parameters as vp
# import virtual_parameters as vp
import copy
import tethys_utils as tu

##############################################
### Parameters

ds_cols = ['feature', 'parameter', 'frequency_interval', 'aggregation_statistic', 'units', 'era5_standard_name',  'wrf_standard_name', 'cf_standard_name', 'scale_factor']

base_dir = os.path.realpath(os.path.dirname(__file__))

########################################
### Main class


class Processor(object):
    """

    """

    ## Initialization
    def __init__(self, nc, geopotential_path):
        """

        """
        xr1 = xr.open_mfdataset(nc)

        ### Pre-process the station data
        ## Station_ids
        lat = xr1['latitude'].values
        lon = xr1['longitude'].values

        lon1, lat1 = np.meshgrid(lon, lat)

        def make_stn_id(x, y):
            stn_id = tu.assign_station_id(tu.create_geometry([x, y]))
            return stn_id

        vfunc = np.vectorize(make_stn_id)
        stn_ids = vfunc(lon1, lat1)

        xr1.coords['station_id'] = (('latitude', 'longitude'), stn_ids)

        ## Altitude
        alt1 = xr.open_dataset(geopotential_path)
        alt2 = alt1.squeeze('time').drop('time').sel(longitude=lon, latitude=lat)
        alt3 = alt2['z'] / 9.80665
        alt3.name = 'altitude'
        alt4 = xr.where(alt3 < 0, 0, alt3)
        xr1.coords['altitude'] = alt4

        ## Determine frequency interval
        freq = xr1['time'][:5].to_index()[:5].inferred_freq

        if freq is None:
            raise ValueError('The time frequency could not be determined from the netcdf file.')

        ### Read in mapping table
        mappings = pd.read_csv(os.path.join(base_dir, 'era5_mappings.csv'))
        mappings.set_index('parameter_code', inplace=True)
        mappings['frequency_interval'] = freq

        ### Process base datasets
        dsb = mappings[ds_cols].rename(columns={'scale_factor': 'precision'}).to_dict('index')

        ### Set attrs
        setattr(self, 'data', xr1)
        setattr(self, 'mappings', mappings)
        setattr(self, 'datasets', dsb)
        setattr(self, 'vp', vp)

        pass


    def __repr__(self):
        return repr(self.data)


    def get_results(self, parameter_code=None, station_id_index=True):
        """

        """
        if (not hasattr(self, 'parameter_code')) and (parameter_code is None):
            raise ValueError('Either define a parameter_code or run the build_dataset method prior to the get_results method.')

        if isinstance(parameter_code, str):
            map1 = self.mappings.loc[parameter_code].copy()
        else:
            map1 = self.mappings.loc[self.parameter_code].copy()

        if isinstance(map1['function'], str):
            meth = getattr(self.vp, map1['function'])
            res1 = meth(self.data)
        else:
            res1 = self.data[map1['era5_standard_name']]

        res1.name = self.param_dataset['parameter']
        if station_id_index:
            res2 = res1.stack(id=['longitude', 'latitude']).set_index(id='station_id').rename(id='station_id').copy()
        else:
            res2 = res1.copy()

        setattr(self, 'param_data', res2)
        setattr(self, 'param_map', map1)

        return res2


    def build_dataset(self, parameter_code, owner='ECMWF', product_code='reanalysis-era5-land', data_license='https://apps.ecmwf.int/datasets/licences/copernicus/', attribution='Generated using Copernicus Climate Change Service Information 2021', method='simulation', result_type="time_series_grid"):
        """

        """
        if parameter_code not in self.datasets:
            raise ValueError('parameter_code ' + parameter_code + ' is not available. Check the datasets dict for the available parameter codes.')

        ## Remove prior stored objects
        if hasattr(self, 'parameter_code'):
            delattr(self, 'parameter_code')
        if hasattr(self, 'param_dataset'):
            delattr(self, 'param_dataset')
        if hasattr(self, 'param_data'):
            delattr(self, 'param_data')
        if hasattr(self, 'param_map'):
            delattr(self, 'param_map')
        if hasattr(self, 'data_dict'):
            delattr(self, 'data_dict')
        if hasattr(self, 'run_date_dict'):
            delattr(self, 'run_date_dict')

        ## Build the dataset
        datasets = self.datasets

        ds = datasets[parameter_code].copy()
        ds.update({'owner': owner, 'product_code': product_code, 'license': data_license, 'attribution': attribution, 'utc_offset': '0H', "result_type": result_type, 'method': method})

        ##  Assign the dataset_id
        ds1 = tu.assign_ds_ids([ds])[0]

        setattr(self, 'param_dataset', ds1)
        setattr(self, 'parameter_code', parameter_code)

        return ds1


    def package_results(self, run_date=None):
        """

        """
        start1 = pd.Timestamp.now('utc').round('s')
        print('start: ' + str(start1))

        ## prepare all of the input data
        if not hasattr(self, 'param_dataset'):
            raise ValueError('Run build dataset before save_results.')

        ds1 = self.param_dataset.copy()

        if not hasattr(self, 'param_data'):
            data1 = self.get_results().copy()
        else:
            data1 = self.param_data.copy()

        map1 = self.param_map.copy()
        data1['height']= map1['height']
        encoding = {ds1['parameter']: map1[['scale_factor', 'add_offset', 'dtype', '_FillValue']].dropna().to_dict()}
        # attrs = {ds1['parameter']: ds1.copy()}
        # remote = {'bucket': bucket, 'connection_config': conn_config}

        ds_id = ds1['dataset_id']
        ds2 = ds1.copy()
        ds2['properties'] = {'encoding': encoding}

        max_run_date_key = tu.make_run_date_key(run_date)
        run_date_dict = {ds_id: max_run_date_key}

        # run_date_dict = tu.process_run_date(3, [ds1], remote, run_date=run_date)
        # max_run_date_key = max(list(run_date_dict.values()))

        ## Create the data_dict
        ds_id = ds1['dataset_id']
        data_dict = {ds_id: []}

        ## Prepare data
        stn_ids = data1.station_id.values.tolist()

        # for s in stn_ids[:1000]:
        for s in stn_ids:
            print(s)
            data2 = data1.sel(station_id=s).copy()

            lat = float(data2['lat'].values)
            lon = float(data2['lon'].values)
            alt = round(float(data2['altitude'].values), 3)

            geo1 = {"coordinates": [lon, lat], "type": "Point"}

            stn_data = {'geometry': geo1, 'altitude': alt, 'station_id': s, 'virtual_station': True}

            df1 = data2.drop(['lat', 'lon', 'altitude', 'station_id']).to_dataframe().reset_index()
            df2 = df1.drop_duplicates('time', keep='first')

            tu.prepare_results(data_dict, [ds2], stn_data, df2, max_run_date_key,  other_closed='left', discrete=False)

        setattr(self, 'data_dict', data_dict)
        setattr(self, 'run_date_dict', run_date_dict)
        data1.close()
        del data1
        data2.close()
        del data2
        print('Finished packaging the data')


    def save_results(self, processing_code, remote, public_url=None, threads=30):
        """

        """
        if not hasattr(self, 'data_dict'):
            raise ValueError('The package_results method must be run prior to saving data.')

        tu.update_results_s3(processing_code, self.data_dict, self.run_date_dict, remote, threads=threads, public_url=public_url)

        print('Finished saving data!')


    def update_stations_datasets(self, remote, threads=60):
        """

        """
        s3 = tu.s3_connection(remote['connection_config'], threads)

        ds = self.param_dataset.copy()

        ds_new = tu.put_remote_dataset(s3, remote['bucket'], ds)
        ds_stations = tu.put_remote_agg_stations(s3, remote['bucket'], ds['dataset_id'], threads)

        ### Aggregate all datasets for the bucket
        ds_all = tu.put_remote_agg_datasets(s3, remote['bucket'], threads)

        print('--Success!')
