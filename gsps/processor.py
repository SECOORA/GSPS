# Sets a timer whenever a new glider file is closed.
# New file is appended to a list.
# When timer expires, full file list is published to ZMQ
#
# ZMQ Multi-part Message Format:
# * Glider Name
# * UNIX Timestamp
# * Folder Path
# * List of New Data Files
#
# By: Michael Lindemuth
# University of South Florida
# College of Marine Science
# Ocean Technology Group

from pyinotify import(
    ProcessEvent
)
from threading import Timer

import logging
logger = logging.getLogger("GSPS")

import zmq
from datetime import datetime


class GliderFileProcessor(ProcessEvent):
    def __init__(self, timeout=600):
        ProcessEvent.__init__(self)
        self.files = []
        self.timeout = timeout
        self.timer = None

        # Create ZMQ context and socket for publishing files
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind("tcp://*:8008")

    def stop(self):
        if self.timer is not None:
            self.timer.cancel()
        self.publish_files()

    def publish_files(self):
        logger.info("Publishing files via ZMQ")
        self.socket.send(datetime.utcnow().isoformat())
        self.files = []

    def set_timer(self, event):
        if len(event.name) > 0 and event.name[0] is not '.':
            full_path = "%s/%s" % (event.pathname, event.name)
            self.files.append(full_path)
            logger.info("File %s closed" % full_path)

            if self.timer is not None:
                self.timer.cancel()

            self.timer = Timer(self.timeout, self.publish_files)
            self.timer.start()

            logger.info(
                "Setting timer to timeout in %d minutes" % (self.timeout/60)
            )

    def process_IN_CLOSE_WRITE(self, event):
        self.set_timer(event)

    def process_IN_CLOSE_NOWRITE(self, event):
        self.set_timer(event)
