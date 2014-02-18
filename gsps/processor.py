# Sets a timer whenever a new glider file is closed.
# New file is appended to a list.
# When timer expires, full file list is published to ZMQ
#
# ZMQ Multi-part Message Format:
# * Glider Name
# * UNIX Timestamp
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

from glider_binary_data_reader.glider_bd_reader import (
    GliderBDReader,
    MergedGliderBDReader
)


FLIGHT_SCIENCE_PAIRS = [('dbd', 'ebd'), ('sbd', 'tbd'), ('mbd', 'nbd')]


class GliderFileProcessor(ProcessEvent):
    def __init__(self, timeout=300):
        ProcessEvent.__init__(self)
        self.glider_data = {}
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

        set_timestamp = datetime.utcnow()
        for glider, fileTypes in self.glider_data.items():
            for pair in FLIGHT_SCIENCE_PAIRS:
                if pair[0] in fileTypes and pair[1] in fileTypes:
                    self.socket.send_json({
                        'message_type': 'set_start',
                        'start': set_timestamp.isoformat(),
                        'flight_type': pair[0],
                        'science_type': pair[1],
                        'glider': glider
                    })
                    flight_reader = GliderBDReader(
                        fileTypes['path'],
                        pair[0],
                        fileTypes[pair[0]]
                    )
                    science_reader = GliderBDReader(
                        fileTypes['path'],
                        pair[1],
                        fileTypes[pair[1]]
                    )
                    merged_reader = MergedGliderBDReader(
                        flight_reader, science_reader
                    )
                    for value in merged_reader:
                        self.socket.send_json({
                            'message_type': 'set_data',
                            'data': value
                        })
                    self.socket.send_json({
                        'message_type': 'set_end',
                        'start': set_timestamp.isoformat(),
                        'flight_type': pair[0],
                        'science_type': pair[1],
                        'glider': glider
                    })

    def set_timer(self, event):
        if len(event.name) > 0 and event.name[0] is not '.':
            logger.info("File %s closed" % event.pathname)

            # Add full path to glider data queue
            glider_name = event.path[event.path.rfind('/'):]
            if glider_name not in self.glider_data:
                self.glider_data[glider_name] = {}
                self.glider_data[glider_name]['path'] = event.path

            fileType = event.name[-3:]
            if fileType not in self.glider_data[glider_name]:
                self.glider_data[glider_name][fileType] = []

            self.glider_data[glider_name][fileType].append(event.name)

            # Reset the timer for announcing the glider data queue
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
