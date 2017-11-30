# Collectd mongodb oplog usage stats

The scrict is a collectd plugin that outputs the oplog data rate usage per collection.

## How it works
* Fetch the docs from the oplog that were added since the last sample (first sample starts at the script's run time).
* We count docs size per ns since we were started
* collectd outputs this data as datarate (the metric type is COUNTER).
* Run interval is determined by collectd

PROBLEM: collectd interval is usually 10s, while default graphite
bucket is 60s, so only the latest sample is retained by graphite :(
