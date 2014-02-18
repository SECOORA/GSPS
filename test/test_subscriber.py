# Tests the gsps publisher by subscribing to the queue.
# Prints any messages received
#
# By: Michael Lindemuth <mlindemu@usf.edu>
# University of South Florida
# College of Marine Science
# Ocean Technology Group

import argparse
import zmq

import sys


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
    socket.connect("tcp://localhost:%d" % args.port)
    socket.setsockopt(zmq.SUBSCRIBE, '')

    while True:
        try:
            print socket.recv_json()
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    sys.exit(main())
