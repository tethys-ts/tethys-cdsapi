#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 31 11:28:40 2023

@author: mike
"""
import os

##########################################
### Parameters


#################################################
### Functions


def concat(input_files, output_file, buffer_size: int=2**20):
    """
    Concatenate multiple binary files to another file. Must be on disk as all files must have file descriptors. This can only be used on Linux OS's. This is primarily for concatenating grib files. This will definitely not work for netcdf4 files!

    Parameters
    ----------
    input_files : iterator of file objects that have file descriptors
    output_file : object that can be opened by "open" and has a file descriptor
    buffer_size : int
        The read/write buffer size.

    Returns
    -------
    None
    """
    with open(output_file, 'wb') as f:
        for file in input_files:
            with open(file, 'rb') as f2:
                b = buffer_size
                while b != 0:
                    b = os.sendfile(f.fileno(), f2.fileno(), None, buffer_size)