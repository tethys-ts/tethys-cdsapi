#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:03:33 2021

@author: mike
"""
import os
import glob
import xarray as xr
import pandas as pd
import numpy as np
from tethys_cdsapi import virtual_parameters as vp
# import virtual_parameters as vp
# import copy
import tethys_utils as tu
# from multiprocessing.pool import ThreadPool, Pool
from tethys_cdsapi.utils import param_file_mappings

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
    def __init__(self, data_path, parameter_code):
        """

        """
        if not parameter_code in param_file_mappings:
            raise ValueError('parameter_code not available.')

        ### Read in mapping table
        mappings = pd.read_csv(os.path.join(base_dir, 'era5_mappings.csv'))
        mappings.set_index('parameter_code', inplace=True)
        mappings['frequency_interval'] = 'H'

        ### Process base datasets
        dsb = mappings[ds_cols].rename(columns={'scale_factor': 'precision'}).to_dict('index')
        map1 = mappings.loc[parameter_code].copy()

        nc_list1 = [os.path.join(data_path, f) for f in param_file_mappings[parameter_code]]

        nc_list2 = []
        for f in nc_list1:
            f2 = glob.glob(f)
            nc_list2.extend(f2)

        nc_list2.sort()

        # xr1 = xr.open_mfdataset(nc_list2, parallel=True, chunks={'time': 8760})
        # xr1 = xr.open_mfdataset(nc_list2, concat_dim="time", data_vars='minimal', coords='minimal', compat='override')
        # xr1 = xr.open_mfdataset(nc_list2, chunks={'time': 1}, concat_dim="time", data_vars='minimal', coords='minimal', compat='override')
        # xr1 = xr.open_mfdataset(nc_list2)
        xr1 = xr.open_mfdataset(nc_list2, chunks={'longitude': 1, 'latitude': 1})
        # d1 = xr1.sel(longitude=166.3, latitude=-34.3)
        # d2 = d1.load()

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
        alt1 = xr.open_mfdataset(os.path.join(data_path, 'geo_*.nc'))
        alt2 = alt1.squeeze('time').drop('time').sel(longitude=lon, latitude=lat).load()
        alt3 = alt2['z'] / 9.80665
        alt3.name = 'altitude'
        alt4 = xr.where(alt3 < 0, 0, alt3)
        xr1.coords['altitude'] = alt4

        xr1 = xr1.rename({'longitude': 'lon', 'latitude': 'lat'})
        xr1.coords['height'] = map1['height']

        data1 = xr1

        ### Iterate through the files
        # xr_m_list = []
        # for nc_files in nc_list1:
        #     print(nc_files)

        #     f2 = glob.glob(nc_files)

        #     xr_list = []
        #     for nc_file in f2:

        #         xr1 = xr.open_dataset(nc_file)
        #         ### Pre-process the station data
        #         ## Station_ids
        #         lat = xr1['latitude'].values
        #         lon = xr1['longitude'].values

        #         lon1, lat1 = np.meshgrid(lon, lat)

        #         def make_stn_id(x, y):
        #             stn_id = tu.assign_station_id(tu.create_geometry([x, y]))
        #             return stn_id

        #         vfunc = np.vectorize(make_stn_id)
        #         stn_ids = vfunc(lon1, lat1)

        #         xr1.coords['station_id'] = (('latitude', 'longitude'), stn_ids)

        #         ## Altitude
        #         alt1 = xr.open_mfdataset(os.path.join(data_path, 'geo_*.nc'))
        #         alt2 = alt1.squeeze('time').drop('time').sel(longitude=lon, latitude=lat).load()
        #         alt3 = alt2['z'] / 9.80665
        #         alt3.name = 'altitude'
        #         alt4 = xr.where(alt3 < 0, 0, alt3)
        #         xr1.coords['altitude'] = alt4

        #         xr1 = xr1.rename({'longitude': 'lon', 'latitude': 'lat'})
        #         xr1.coords['height'] = map1['height']

        #         # xr2 = self.get_results(xr1, map1, vp, False)

        #         xr_list.append(xr1)

        # d1 = xr1.sel(lon=166.3, lat=-34.3)
        # d2 = d1.load()

        ## Determine frequency interval
        freq = xr1['time'][:5].to_index()[:5].inferred_freq

        if freq is None:
            raise ValueError('The time frequency could not be determined from the netcdf file.')

        ### Set attrs
        setattr(self, 'data', xr1)
        # setattr(self, 'data_list', xr_list)
        # setattr(self, 'mappings', mappings)
        setattr(self, 'param_map', map1)
        setattr(self, 'datasets', dsb)
        setattr(self, 'vp', vp)
        setattr(self, 'parameter_code', parameter_code)

        pass


    def __repr__(self):
        return repr(self.data)


    @staticmethod
    def get_results(data, mapping, vp, station_id_index=False):
        """

        """
        # if (not hasattr(self, 'parameter_code')) and (parameter_code is None):
        #     raise ValueError('Either define a parameter_code or run the build_dataset method prior to the get_results method.')

        # if isinstance(parameter_code, str):
        #     map1 = self.mappings.loc[parameter_code].copy()
        # else:

        if isinstance(mapping['function'], str):
            meth = getattr(vp, mapping['function'])
            res1 = meth(data)
        else:
            res1 = data[mapping['era5_standard_name']]

        res1.name = mapping['parameter']
        if station_id_index:
            res1['lat1'] = res1['lat']
            res1['lon1'] = res1['lon']
            res2 = res1.stack(id=['lon', 'lat']).set_index(id='station_id').rename({'id': 'station_id', 'lon1': 'lon', 'lat1': 'lat'}).copy()
        else:
            res2 = res1.copy()

        return res2


    def build_dataset(self, owner='ECMWF', product_code='reanalysis-era5-land', data_license='https://apps.ecmwf.int/datasets/licences/copernicus/', attribution='Generated using Copernicus Climate Change Service Information 2021', method='simulation', result_type="time_series_grid"):
        """

        """
        # if parameter_code not in self.datasets:
        #     raise ValueError('parameter_code ' + parameter_code + ' is not available. Check the datasets dict for the available parameter codes.')

        ## Remove prior stored objects
        # if hasattr(self, 'parameter_code'):
        #     delattr(self, 'parameter_code')
        # if hasattr(self, 'param_dataset'):
        #     delattr(self, 'param_dataset')
        # if hasattr(self, 'param_data'):
        #     delattr(self, 'param_data')
        # if hasattr(self, 'param_map'):
        #     delattr(self, 'param_map')
        # if hasattr(self, 'data_dict'):
        #     delattr(self, 'data_dict')
        # if hasattr(self, 'run_date_dict'):
        #     delattr(self, 'run_date_dict')

        ## Build the dataset
        datasets = self.datasets

        ds = datasets[self.parameter_code].copy()
        ds.update({'owner': owner, 'product_code': product_code, 'license': data_license, 'attribution': attribution, 'utc_offset': '0H', "result_type": result_type, 'method': method})

        ##  Assign the dataset_id
        ds1 = tu.assign_ds_ids([ds])[0]

        setattr(self, 'param_dataset', ds1)
        # setattr(self, 'parameter_code', parameter_code)

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

        # if not hasattr(self, 'param_data'):
        #     data1 = self.get_results().copy()
        # else:
        #     data1 = self.param_data.copy()
        # data1 = self.data_list
        data1 = self.data.copy()

        map1 = self.param_map.copy()
        # data1['height'] = map1['height']
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
        # t2 = data1[0].isel(time=10)
        t2 = data1.isel(time=10)
        var = list(t2.data_vars)[0]
        stns = t2[var].to_dataframe().dropna().drop([var, 'time', 'height'], axis=1).reset_index()
        # stns = data1[0].station_id.to_dataframe().reset_index().iloc[:, :-1]

        # stns_list = [s for i, s in stns.iterrows()]

        # for s in stn_ids[:1000]:

        get_results = self.get_results
        vp = self.vp

        for i, s in stns.iterrows():
        # def process_results(s):
            stn_id = str(s.station_id)
            print(stn_id)

            lat = float(s['lat'])
            lon = float(s['lon'])
            alt = round(float(s['altitude']), 3)

            geo1 = {"coordinates": [lon, lat], "type": "Point"}

            stn_data = {'geometry': geo1, 'altitude': alt, 'station_id': stn_id, 'virtual_station': True}

            d2 = data1.sel(lat=lat, lon=lon).copy()
            data3 = get_results(d2, map1, vp)

            # d_list = []
            # for data in data1:
            #     d2 = data.sel(lat=lat, lon=lon).copy().load()
            #     data2 = get_results(d2, map1, vp)
            #     d_list.append(data2)

            # data3 = xr.concat(d_list, dim='time')

            df1 = data3.drop(['lat', 'lon', 'altitude', 'station_id']).to_dataframe().reset_index()
            df2 = df1.drop_duplicates('time', keep='first').dropna()

            data3.close()
            del data3

            if not df2.empty:
                tu.prepare_results(data_dict, [ds2], stn_data, df2, max_run_date_key, other_closed='left', discrete=False)

        # with ThreadPool(threads) as pool:
        #     output = pool.map(process_results, stns_list)
        #     pool.close()
        #     pool.join()

        setattr(self, 'data_dict', data_dict)
        setattr(self, 'run_date_dict', run_date_dict)
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

        print('Finished updating the stations and datasets!')
