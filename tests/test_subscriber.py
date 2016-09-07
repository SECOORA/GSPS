#!/usr/bin/env python

# Tests the gsps publisher by subscribing to the queue.
# Prints any messages received
#
# By: Michael Lindemuth <mlindemu@usf.edu>
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import sys
import argparse

import zmq

import logging
from gsps import logger
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def main():
    parser = argparse.ArgumentParser(description=(
        'Test the gsps publisher by subscribing to the queue.  '
        'Prints all messages received.'
    ))
    parser.add_argument('--port', type=int, default=8008,
                        help='TCP port to subscribe')
    args = parser.parse_args()

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://127.0.0.1:{:d}'.format(args.port))
    logger.info("Listening...")
    socket.setsockopt(zmq.SUBSCRIBE, b'')

    while True:
        try:
            logger.info(socket.recv_json())
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    sys.exit(main())
