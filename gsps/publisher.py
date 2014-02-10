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
    IN_CLOSE_WRITE
)

from processor import GliderFileProcessor


def main():
    parser = argparse.ArgumentParser(
        description="Monitor a directory for new glider data.  "
                    "Announce changes via ZMQ."
    )
    parser.add_argument(
        "glider_directory_path",
        help="Path to configuration file"
    )
    args = parser.parse_args()

    # Setup logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s "
                                  "- %(levelname)s - %(message)s")
    log_handler = logging.FileHandler('/var/log/gsps/gsps.log')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)

    wm = WatchManager()
    mask = IN_CLOSE_WRITE
    wdd = wm.add_watch(args.glider_directory_path, mask,
                       rec=True, auto_add=True)

    processor = GliderFileProcessor(10)
    notifier = Notifier(wm, processor)

    def handler(signum, frame):
        wm.rm_watch(wdd.values())
        processor.stop()
        notifier.stop()

    signal.signal(signal.SIGTERM, handler)

    pid_file = '/var/run/gsps/gsps.pid'
    try:
        logger.info("Starting")
        notifier.loop(daemonize=False, pid_file=pid_file)
    except NotifierError, err:
        logger.error('Unable to start notifier loop: %s' % err)
        return 0

    logger.info("Glider System Publication Service Exited Successfully")
    return 1

if __name__ == '__main__':
    sys.exit(main())
