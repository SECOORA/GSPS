#!/usr/bin/env python

import os
import json
import shutil
import tempfile
from glob import glob
from threading import Thread

import numpy as np
from netCDF4 import default_fillvals as NC_FILL_VALUES

from gutils.nc import open_glider_netcdf

from gutils.yo.filters import (
    filter_profile_depth,
    filter_profile_time,
    filter_profile_distance,
    filter_profile_number_of_points
)
from gutils.yo import find_yo_extrema
from gutils.gps import interpolate_gps
from gutils.ctd import calculate_density
from gutils.ctd import calculate_practical_salinity

from gsps.nc.generators import (
    generate_global_attributes,
    generate_filename,
    generate_set_key
)

import logging
logger = logging.getLogger(__name__)


class GliderDataset(object):
    """Represents a complete glider dataset
    """

    def __init__(self, handler_dataset):
        self.glider = handler_dataset['glider']
        self.segment = handler_dataset['segment']
        self.headers = handler_dataset['headers']
        self.__parse_lines(handler_dataset['lines'])
        self.__interpolate_glider_gps()
        self.__calculate_salinity_and_density()
        self.__calculate_position_uv()

    def __interpolate_glider_gps(self):
        if 'm_gps_lat-lat' in self.data_by_type:
            gps = interpolate_gps(
                self.times,
                self.data_by_type['m_gps_lat-lat'],
                self.data_by_type['m_gps_lon-lon']
            )
            self.data_by_type['lat-lat'] = gps[:, 1]
            self.data_by_type['lon-lon'] = gps[:, 2]

    def __calculate_salinity_and_density(self):
        if 'sci_water_cond-s/m' in self.data_by_type:
            dataset = np.column_stack((
                self.times,
                self.data_by_type['sci_water_cond-s/m'],
                self.data_by_type['sci_water_temp-degc'],
                self.data_by_type['sci_water_pressure-bar']
            ))
            salinity_dataset = calculate_practical_salinity(dataset)
            density_dataset = calculate_density(
                salinity_dataset,
                self.data_by_type['lat-lat'],
                self.data_by_type['lon-lon']
            )
            density_dataset[np.isnan(density_dataset[:, 7]), 7] = (
                NC_FILL_VALUES['f8']
            )
            density_dataset[np.isnan(density_dataset[:, 9]), 9] = (
                NC_FILL_VALUES['f8']
            )
            self.data_by_type['salinity-psu'] = density_dataset[:, 7]
            self.data_by_type['density-kg/m^3'] = density_dataset[:, 9]

    def __parse_lines(self, lines):
        self.time_uv = NC_FILL_VALUES['f8']
        self.times = []
        self.data_by_type = {}

        for header in self.headers:
            self.data_by_type[header] = []

        for line in lines:
            self.times.append(line['timestamp'])
            for key in self.data_by_type.keys():
                if key in line:
                    datum = line[key]
                    if key == 'm_water_vx-m/s':
                        self.time_uv = line['timestamp']
                else:
                    datum = NC_FILL_VALUES['f8']
                self.data_by_type[key].append(datum)

    def calculate_profiles(self):
        profiles = []
        if 'm_depth-m' in self.data_by_type:
            dataset = np.column_stack((
                self.times,
                self.data_by_type['m_depth-m']
            ))
            profiles = find_yo_extrema(dataset)
            profiles = filter_profile_depth(profiles)
            profiles = filter_profile_time(profiles)
            profiles = filter_profile_distance(profiles)
            profiles = filter_profile_number_of_points(profiles)

        return profiles[:, 2]

    def __calculate_position_uv(self):
        dataset = np.column_stack((
            self.times,
            self.data_by_type['lat-lat'],
            self.data_by_type['lon-lon']
        ))

        i = np.min(dataset[:, 0] - self.time_uv).argmin()
        self.data_by_type['lat_uv-lat'] = [dataset[i, 1]]
        self.data_by_type['lon_uv-lon'] = [dataset[i, 2]]


def write_netcdf(configs, sets, set_key):
    dataset = GliderDataset(sets[set_key])

    # No longer need the dataset stored by handlers
    del sets[set_key]

    global_attributes = (
        generate_global_attributes(configs, dataset)
    )

    _, tmp_path = tempfile.mkstemp(suffix='.nc')
    with open_glider_netcdf(tmp_path, 'w') as glider_nc:
        glider_nc.set_global_attributes(global_attributes)
        glider_nc.set_platform(
            configs[dataset.glider]['deployment']['platform']
        )
        glider_nc.set_trajectory_id(1)
        glider_nc.set_segment_id(dataset.segment)
        glider_nc.set_datatypes(configs['datatypes'])
        glider_nc.set_instruments(configs[dataset.glider]['instruments'])
        glider_nc.set_times(dataset.times)

        # Insert time_uv parameters
        glider_nc.set_time_uv(dataset.time_uv)

        glider_nc.set_profile_ids(dataset.calculate_profiles())
        for datatype, data in dataset.data_by_type.items():
            glider_nc.insert_data(datatype, data)

    deployment_path = os.path.join(
        configs['output_directory'],
        configs[dataset.glider]['deployment']['directory']
    )

    if not os.path.exists(deployment_path):
        os.mkdir(deployment_path)

    filename = generate_filename(configs, dataset)
    file_path = os.path.join(deployment_path, filename)
    shutil.move(tmp_path, file_path)

    logger.info("Datafile written to %s" % file_path)


def handle_set_start(configs, sets, message):
    """Handles the set start message from the GSPS publisher

    Initializes the new dataset store in memory
    """
    set_key = generate_set_key(message)

    sets[set_key] = {
        'glider': message['glider'],
        'segment': message['segment'],
        'headers': [],
        'lines': []
    }

    for header in message['headers']:
        key = header['name'] + '-' + header['units']
        sets[set_key]['headers'].append(key)

    logger.info(
        "Dataset start for %s @ %s"
        % (message['glider'], message['start'])
    )


def handle_set_data(configs, sets, message):
    """Handles all new data coming in for a GSPS dataset

    All datasets must already have been initialized by a set_start message.
    Appends new data lines to the set lines variable.
    """
    set_key = generate_set_key(message)

    if set_key in sets:
        sets[set_key]['lines'].append(message['data'])
    else:
        logger.error(
            "Unknown dataset passed for key glider %s dataset @ %s"
            % (message['glider'], message['start'])
        )


def handle_set_end(configs, sets, message):
    """Handles the set_end message coming from GSPS

    Checks for empty dataset.  If not empty, it hands
    off dataset to thread.  Thread writes NetCDF data to
    new file in output directory.
    """

    set_key = generate_set_key(message)

    if set_key in sets:
        if len(sets[set_key]['lines']) == 0:
            logger.info(
                "Empty set: for glider %s dataset @ %s"
                % (message['glider'], message['start'])
            )
            return  # No data in set, do nothing

        thread = Thread(
            target=write_netcdf,
            args=(configs, sets, set_key)
        )
        thread.start()
        thread.join()

    logger.info(
        "Dataset end for %s @ %s.  Processing..."
        % (message['glider'], message['start'])
    )


message_handlers = {
    'set_start': handle_set_start,
    'set_data': handle_set_data,
    'set_end': handle_set_end
}


def load_configs(configs_directory):
    configs = {}

    jsonfiles = glob(
        os.path.join(
            configs_directory,
            '**',
            '*.json'
        )
    )

    for filename in jsonfiles:
        with open(filename, 'r') as f:
            try:
                conf = json.loads(f.read())
            except BaseException:
                logger.exception('Error processing {}'.format(filename))
                continue
            folder = os.path.basename(os.path.dirname(filename))
            key = os.path.basename(os.path.splitext(filename)[0])
            if folder not in configs:
                configs[folder] = {}
            configs[folder][key] = conf

    return configs
