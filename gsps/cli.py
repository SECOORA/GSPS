#!/usr/bin/env python

# Monitors a glider directory for changes.
# When a change occurs, it is published to a ZMQ queue.
# The format of these ZMQ mulit-part messages is as follows:
#
# ZMQ Message Format:
# * Glider Name
# * UNIX Timestamp
# * Folder Path
# * List of New Data Files
#
# By: Michael Lindemuth <mlindemu@usf.edu>
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import os
import sys
import argparse

from pyinotify import (
    WatchManager,
    Notifier,
    NotifierError,
    IN_CLOSE_WRITE,
    IN_MOVED_TO
)

from gsps.processor import GliderFileProcessor

import logging
logging.captureWarnings(True)
logger = logging.getLogger(__name__)


def main():
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
    logging.getLogger('py.warnings').setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(
        description="Monitor a directory for new glider data. "
                    "Announce changes via ZMQ."
    )
    parser.add_argument(
        "-d",
        "--data_path",
        help="Path to Glider data directory",
        default=os.environ.get('GDB_DATA_DIR')
    )
    parser.add_argument(
        "--zmq_url",
        help='Port to publish ZMQ messages on. '
             'Default is "tcp://127.0.0.1:44444".',
        default=os.environ.get('ZMQ_URL', 'tcp://127.0.0.1:44444')
    )
    parser.add_argument(
        "--daemonize",
        help="To daemonize or not to daemonize",
        type=bool,
        default=False
    )

    args = parser.parse_args()

    if not args.data_path:
        logger.error("Please provide a --data_path attribute or set the GDB_DATA_DIR "
                     "environmental variable")
        sys.exit(parser.print_usage())

    monitor_path = args.data_path
    if monitor_path[-1] == '/':
        monitor_path = monitor_path[:-1]

    wm = WatchManager()
    mask = IN_MOVED_TO | IN_CLOSE_WRITE
    wm.add_watch(
        args.data_path,
        mask,
        rec=True,
        auto_add=True
    )

    processor = GliderFileProcessor(zmq_url=args.zmq_url)
    notifier = Notifier(wm, processor)

    try:
        logger.info("Watching {}\nPublishing to {}".format(
            args.data_path,
            args.zmq_url)
        )
        notifier.loop(daemonize=args.daemonize)
    except NotifierError:
        logger.exception('Unable to start notifier loop')
        return 1

    logger.info("GSPS Exited Successfully")
    return 0

if __name__ == '__main__':
    sys.exit(main())
