# Glider Singleton Publishing Service (GSPS)

[![Build Status](https://travis-ci.org/SECOORA/GSPS.svg?branch=master)](https://travis-ci.org/SECOORA/GSPS)

Watches a directory for new *db flight/science files and publishes the data to a ZeroMQ socket. Includes a command line tool to listen to the ZeroMQ socket and write netCDF files.


## Installation

Available through [`conda`](http://conda.pydata.org/docs/install/quick.html). This library requires Python 3.5 or above.

```
$ conda create -n sgs python=3.5
$ source activate sgs
$ conda install -c axiom-data-science gsps
```

## `gsps-cli`

#### CLI

```bash
$ gsps-cli --help
```

You can specify the ZeroMQ socket and MongoDB connection via command line
arguments or environmental variables:

```bash
$ gsps-cli -d /data --zmg_url tcp://127.0.0.1:9000
Watching /data
Publishing to tcp://127.0.0.1:9000
```

```bash
$ export ZMQ_URL="tcp://127.0.0.1:9000"
$ gsps-cli -d /data
Watching /data
Publishing to tcp://127.0.0.1:9000
```

```bash
$ ZMQ_URL="tcp://127.0.0.1:9000" gsps-cli -d /data
Watching /data
Publishing to tcp://127.0.0.1:9000
```

#### Docker

The docker image uses `gsps-cli` internally. Set the `ZMQ_URL` variable as needed when calling `docker run`. You most likely want to keep `ZQM_URL` to the default unless you want to change the default port from `44444`.

```bash
$ docker run -it \
    -name sgs-gsps \
    -e "ZMQ_URL=tcp://127.0.0.1:9000" \
    axiom/gsps
Watching /data
Publishing to tcp://127.0.0.1:9000
```


## `gsps2nc`

#### CLI

```bash
$ gsps2nc --help
```

You can specify the ZeroMQ socket via command line arguments or environmental variables:

```bash
$ gsps2nc --zmg_url tcp://127.0.0.1:9000 --configs /config --output /output
Loading configuration from /config
Listening to tcp://127.0.0.1:9000
Saving to /output
```

```bash
$ export ZMQ_URL="tcp://127.0.0.1:9000"
$ export GSPS2NC_CONFIG="/config"
$ export GSPS2NC_OUTPUT="/output"
$ gsps2nc
Loading configuration from /config
Listening to tcp://127.0.0.1:9000
Saving to /output
```

```bash
$ ZMQ_URL="tcp://127.0.0.1:9000" GSPS2NC_CONFIG="/config" GSPS2NC_OUTPUT="/output" gsps2nc
Loading configuration from /config
Listening to tcp://127.0.0.1:9000
Saving to /output
```


#### Docker

The docker image uses `gsps2nc` internally. Set the `ZMQ_URL` variable as needed when calling `docker run`. You want to point `ZMQ_URL` to the socket where the GSPS system is publishing.

By default, the configuration is loaded from `/config` and the data is output
to `/output`. You can change these on run by setting the `GSPS2NC_CONFIG` and
`GSPS2NC_OUTPUT` variables. Be sure to mount volumes to these places with your data.

```bash
$ docker run -it \
    -name sgs-gsps2nc \
    -v /my/config:/config:ro \
    -v /my/output:/output \
    -e "ZMQ_URL=tcp://127.0.0.1:9000" \
    axiom/gsps2nc
Loading configuration from /config
Listening to tcp://127.0.0.1:9000
Saving to /output
```


# SECOORA Glider System (SGS)

This package is part of the SECOORA Glider System (SGS) and was originally developed by the [CMS Ocean Technology Group](http://www.marine.usf.edu/COT/) at the University of South Florida. It is now maintained by [SECOORA](http://secoora.org) and [Axiom Data Science](http://axiomdatascience.com).

* [GUTILS](https://github.com/axiom-data-science/GUTILS): A set of Python utilities for post processing glider data.
* [GSPS](https://github.com/axiom-data-science/GSPS): Watches a directory for new *db flight/science files and publishes the data to a ZeroMQ socket.
* [GDAM](https://github.com/axiom-data-science/GDAM): Watches a directory for new *db flight/science files, inserts the data into a MongoDB instance, and publishes the data to a ZeroMQ socket.
