# Announces data from flight and science files as merged
# JSON documents to ZeroMQ
#
# ZMQ JSON Messages:
# * set_start: Announces the start time and glider
#   - Use start time and glider to differentiate sets if necessary
# * set_data: Announces a row of data
# * set_end: Announces the end of a glider data set
#
# By: Michael Lindemuth
# University of South Florida
# College of Marine Science
# Ocean Technology Group

from pyinotify import(
    ProcessEvent
)

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
    def __init__(self, port=8008):
        ProcessEvent.__init__(self)
        self.glider_data = {}

        self.port = port

    def publish_segment_pair(self, glider, path, file_base, pair):
        # Create ZMQ context and socket for publishing files
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind("tcp://*:%d" % self.port)

        segment_id = int(file_base[file_base.rfind('-')+1:file_base.find('.')])

        logger.info(
            "Publishing glider %s segment %d data in %s named %s pair %s"
            % (glider, segment_id, path, file_base, pair)
        )

        set_timestamp = datetime.utcnow()
        flight_file = file_base + pair[0]
        science_file = file_base + pair[1]

        flight_reader = GliderBDReader(
            path,
            pair[0],
            [flight_file]
        )
        science_reader = GliderBDReader(
            path,
            pair[1],
            [science_file]
        )
        merged_reader = MergedGliderBDReader(
            flight_reader, science_reader
        )

        socket.send_json({
            'message_type': 'set_start',
            'start': set_timestamp.isoformat(),
            'flight_type': pair[0],
            'flight_file': flight_file,
            'science_file': science_file,
            'science_type': pair[1],
            'glider': glider,
            'segment': segment_id,
            'headers': merged_reader.headers
        })

        for value in merged_reader:
            socket.send_json({
                'message_type': 'set_data',
                'glider': glider,
                'start': set_timestamp.isoformat(),
                'data': value
            })
        socket.send_json({
            'message_type': 'set_end',
            'glider': glider,
            'start': set_timestamp.isoformat(),
        })

        self.glider_data[glider]['files'].remove(flight_file)
        self.glider_data[glider]['files'].remove(science_file)

    def check_for_pair(self, event):
        if len(event.name) > 0 and event.name[0] is not '.':
            # Add full path to glider data queue
            glider_name = event.path[event.path.rfind('/')+1:]
            if glider_name not in self.glider_data:
                self.glider_data[glider_name] = {}
                self.glider_data[glider_name]['path'] = event.path
                self.glider_data[glider_name]['files'] = []

            self.glider_data[glider_name]['files'].append(event.name)

            fileType = event.name[-3:]

            # Check for matching pair
            for pair in FLIGHT_SCIENCE_PAIRS:
                checkFile = None
                if fileType == pair[0]:
                    checkFile = event.name[:-3] + pair[1]
                elif fileType == pair[1]:
                    checkFile = event.name[:-3] + pair[0]

                if checkFile in self.glider_data[glider_name]['files']:
                    try:
                        self.publish_segment_pair(
                            glider_name, event.path, event.name[:-3], pair
                        )
                    except Exception, e:
                        logger.error("Error processing pair %s: %s"
                                     % (event.name[:-3], e))

    def valid_extension(self, name):
        extension = name[name.rfind('.')+1:]
        for pair in FLIGHT_SCIENCE_PAIRS:
            if extension == pair[0] or extension == pair[1]:
                return True

        logger.error("Unrecognized file extension for event: %s" % extension)
        return False

    def process_IN_CLOSE(self, event):
        if self.valid_extension(event.name):
            self.check_for_pair(event)

    def process_IN_MOVED_TO(self, event):
        if self.valid_extension(event.name):
            self.check_for_pair(event)
