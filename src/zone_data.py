from flask import request
from flask_restful import Resource

import pytz
from dateutil import parser
from datetime import datetime, timedelta

import pymongo
from crate import client

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

        # Crate
        self.host = "http://172.18.65.54:4200"
        self.connection = client.connect(self.host)

    def get(self):
        ret_val = []

        try:
            # Crate
            cur_time = request.args.get('date')
            if cur_time is None or cur_time == '0' or cur_time == 'undefined':
                cur_time = datetime.now(tz=self.local_tz)
            else:
                cur_time = self.local_tz.localize(parser.parse(cur_time))
            cur_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date_utc = cur_time.astimezone(pytz.utc)
            #end_date_utc = start_date_utc + timedelta(hours=24)
            end_date_utc = start_date_utc + timedelta(hours=30 * 24)

            # Get power
            topic = 'PNNL/350_BUILDING/METERS/WholeBuildingPowerWithoutShopAirCompressor'
            topic = 'PNNL/SEB/AHU1/OutdoorAirTemperature'
            #start_time = '2018-08-01T00:00:00-07:00'  # US/Pacific time
            #end_time = '2018-08-30T08:00:00-07:00'  # US/Pacific time
            start_time = start_date_utc.strftime('%Y-%m-%dT%H:%M:%S%z')
            end_time = end_date_utc.strftime('%Y-%m-%dT%H:%M:%S%z')
            limit = 100000

            query = "SELECT ts, double_value " \
                    "FROM cetc_infrastructure.data " \
                    "WHERE topic = '{topic}' " \
                    "AND ts BETWEEN '{start}' AND '{end}' " \
                    "ORDER BY ts " \
                    "LIMIT {limit};".format(topic=topic,
                                            start=start_time,
                                            end=end_time,
                                            limit=limit)

            cur = self.connection.cursor()
            cur.execute(query)
            records = cur.fetchall()

            for record in records:
                ts = datetime.fromtimestamp(int(record[0])/1000)
                # ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                ret_val.append({
                    'ts': format_ts(ts),
                    'value': record[1]
                })

            if ret_val is not None:
                ret_val = sorted(ret_val, key=lambda k: k['ts'])

        except Exception, exc:
            print(exc)
            return ret_val

        return ret_val

    def get_zone(self):
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