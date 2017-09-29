from flask import request
from flask_restful import Resource

import pytz
from dateutil import parser
from datetime import datetime, timedelta

import pymongo

from utils import format_ts


class ZoneData(Resource):
    def __init__(self):
        self.batch_size = 10000
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'prod_historian'}
        mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
        mongo_uri = mongo_uri.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_default_database()

        self.local_tz = pytz.timezone('US/Pacific')

    def get(self):
        ret_val = []
        try:
            topic = request.args.get('topic')
            cur_time = request.args.get('date')
            if cur_time is None or cur_time == '0' or cur_time == 'undefined':
                cur_time = datetime.now(tz=self.local_tz)
            else:
                cur_time = self.local_tz.localize(parser.parse(cur_time))

            cur_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date_utc = cur_time.astimezone(pytz.utc)
            end_date_utc = start_date_utc + timedelta(hours=24)
            ts_filter = {
                "$gte": start_date_utc,
                "$lte": end_date_utc
            }

            parts = topic.split(',')
            topic = '/'.join(parts)
            find_params = {
                'topic_name': topic
            }
            data_cur = self.mongodb["topics"].find(find_params)
            records = data_cur[:]

            topic_id = None
            for record in records:
                topic_id = record['_id']

            if topic_id is None:
                return ret_val

            find_params = {
                'topic_id': topic_id,
                'ts': ts_filter
            }

            data_cur = self.mongodb["data"].find(find_params).batch_size(self.batch_size)
            records = data_cur[:]

            for record in records:
                ts = record['ts']
                ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                ret_val.append({
                    'ts': format_ts(ts),
                    'value': record['value']
                })

            if ret_val is not None:
                ret_val = sorted(ret_val, key=lambda k: k['ts'])
        except:
            return ret_val

        return ret_val


class ZoneDataByDate(Resource):
    def __init__(self):
        self.batch_size = 10000
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'prod_historian'}
        mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
        mongo_uri = mongo_uri.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_default_database()

        self.local_tz = pytz.timezone('US/Pacific')

    def get(self, date):
        pass