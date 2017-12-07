import time
import pymongo
import bson
import collectd

class MongoDBOplog(object):
    def __init__(self):
        self.plugin_name = "mongodb-oplog"
        self.client = pymongo.MongoClient()
        self.oplog = self.client.local.oplog.rs
        self.ts = bson.timestamp.Timestamp(int(time.time()),0)
        self.counters = dict()
        self.db_counters = dict()
    def read(self):
        cursor = self.oplog.find({'ts': {'$gt': self.ts}}, oplog_replay=True)

        for doc in cursor:
            self.ts = doc['ts']
            db, collection = doc['ns'].split('.',1)
            size = len(bson.BSON.encode(doc))
            # Update per-collection counters
            if not db in self.counters:
                self.counters[db] = dict()
            if not collection  in self.counters[db]:
                self.counters[db][collection] = 0
            self.counters[db][collection] += size
            # Update per-database counters
            if not db in self.db_counters:
                self.db_counters[db] = 0
            self.db_counters[db] += size

        # Output per-collection counters
        for db in self.counters:
            for collection in self.counters[db]:
                v = collectd.Values()
                v.plugin = self.plugin_name
                v.plugin_instance = db
                v.type = "oplog_datarate_per_collection"
                v.type_instance = collection
                v.values = (self.counters[db][collection],)
                v.dispatch()
        # Output per-database counters
        for db in self.db_counters:
            v = collectd.Values()
            v.plugin = self.plugin_name
            v.plugin_instance = db
            v.type = "oplog_datarate"
            v.values = (self.db_counters[db],)
            v.dispatch()

mongodb_oplog = MongoDBOplog()
collectd.register_read(mongodb_oplog.read)
