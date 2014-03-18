#!/usr/bin/python

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

import argparse
import sys
import signal
import logging
logger = logging.getLogger("GSPS")

from pyinotify import (
    WatchManager,
    Notifier,
    NotifierError,
    IN_CLOSE_WRITE,
    IN_CLOSE_NOWRITE
)

from glider_singleton_publishing_service.processor import GliderFileProcessor


def main():
    parser = argparse.ArgumentParser(
        description="Monitor a directory for new glider data.  "
                    "Announce changes via ZMQ."
    )
    parser.add_argument(
        "glider_directory_path",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--zmq_port",
        help="Port to publish ZMQ messages on.  8008 by default.",
        type=int,
        default=8008
    )
    parser.add_argument(
        "--daemonize",
        help="To daemonize or not to daemonize",
        type=bool,
        default=False
    )
    parser.add_argument(
        "--pid_file",
        help="Where to look for and put the PID file",
        default="./gsps.pid"
    )
    parser.add_argument(
        "--log_file",
        help="Full path of file to log to",
        default="./gsps.log"
    )
    args = parser.parse_args()

    # Setup logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s "
                                  "- %(levelname)s - %(message)s")
    log_handler = logging.FileHandler(args.log_file)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    monitor_path = args.glider_directory_path
    if monitor_path[-1] == '/':
        monitor_path = monitor_path[:-1]

    wm = WatchManager()
    mask = IN_CLOSE_WRITE | IN_CLOSE_NOWRITE
    wdd = wm.add_watch(args.glider_directory_path, mask,
                       rec=True, auto_add=True)

    processor = GliderFileProcessor(args.zmq_port)
    notifier = Notifier(wm, processor)

    def handler(signum, frame):
        wm.rm_watch(wdd.values())
        processor.stop()
        notifier.stop()

    signal.signal(signal.SIGTERM, handler)

    try:
        logger.info("Starting")
        notifier.loop(daemonize=args.daemonize, pid_file=args.pid_file)
    except NotifierError, err:
        logger.error('Unable to start notifier loop: %s' % err)
        return 1

    logger.info("Glider System Publication Service Exited Successfully")
    return 0

if __name__ == '__main__':
    sys.exit(main())
