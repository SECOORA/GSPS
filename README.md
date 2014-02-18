# GSPS

Monitors a Teledyne Webb from-glider directory for any new file updates.  After not receiving any files for 10 minutes, it begins publishing the received data set via ZeroMQ.  Subscribers of this service will most often either convert the data to a flat file format - such as CSV or NetCDF - or insert the new data into a database.

# Usage

Basic usage:

```
git clone https://github.com/USF-COT/GSPS.git
python gsps/listener.py <path to from-glider directory>
```

For usage options type:

```
python gsps/listener.py -h
```

# Dependencies

* Glider Binary Data Reader: https://github.com/USF-COT/glider_binary_data_reader.git
