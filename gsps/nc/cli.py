#!/usr/bin/env python

# Subscribes to the Glider Singleton Publishing Service Socket.
# When a new set is published, it outputs a new NetCDF to a given
# output directory.
#
# By: Michael Lindemuth
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import os
import sys
import zmq
import argparse

from gsps.nc import load_configs, message_handlers

import logging
logging.captureWarnings(True)
logger = logging.getLogger(__name__)


def main():
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logging.getLogger('py.warnings').setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(
        description="Subscribes to the Glider Singleton Publishing Service "
                    "(GSPS) socket.  When a new set is published, it outputs a new "
                    "NetCDF to a given output directory."
    )
    parser.add_argument(
        "--zmq_url",
        help='Port to listen for ZMQ GSPS messages. '
             'Default is "tcp://127.0.0.1:44444".',
        default=os.environ.get('ZMQ_URL', 'tcp://127.0.0.1:44444')
    )
    parser.add_argument(
        "--configs",
        help="Folder to look for NetCDF global and glider "
             "JSON configuration files.  Default is './config'.",
        default=os.environ.get('GSPS2NC_CONFIG', './config')
    )
    parser.add_argument(
        "--output",
        help="Where to place the newly generated netCDF files.",
        default=os.environ.get('GSPS2NC_OUTPUT')
    )

    args = parser.parse_args()

    if not args.output:
        logger.error("Please provide an --output argument or set the "
                     "GSPS2NC_OUTPUT environmental variable")
        sys.exit(parser.print_usage())

    configs_directory = args.configs
    if configs_directory[-1] == '/':
        configs_directory = configs_directory[:-1]
    configs = load_configs(configs_directory)

    output_directory = args.output
    if output_directory[-1] == '/':
        output_directory = output_directory[:-1]
    configs['output_directory'] = output_directory

    configs['zmq_url'] = args.zmq_url

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(configs['zmq_url'])
    socket.setsockopt(zmq.SUBSCRIBE, b'')

    sets = {}

    logger.info("Loading configuration from {}\nListening to {}\nSaving to {}".format(
        args.configs,
        args.zmq_url,
        output_directory)
    )

    while True:
        try:
            message = socket.recv_json()
            if message['message_type'] in message_handlers:
                message_type = message['message_type']
                message_handlers[message_type](configs, sets, message)
        except BaseException as e:
            logger.error("Subscriber exited: {}".format(e))
            break

    logger.info('Stopped')

if __name__ == '__main__':
    sys.exit(main())
