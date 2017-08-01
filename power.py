from flask import Flask, request, current_app
from flask_restful import Resource, Api
from flask_restful.utils import cors

import json
import pymongo
from bson.objectid import ObjectId

from datetime import datetime, timedelta
import pytz
import dateutil.tz
from dateutil import parser

app = Flask(__name__)
api = Api(app, decorators=[cors.crossdomain(origin='*')])


class PowerData(Resource):
    def __init__(self):
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'prod_historian'}
        mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
        mongo_uri = mongo_uri.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_default_database()

        self.local_tz = pytz.timezone('US/Pacific')

    def get(self, resource_id):
        ret_val = []
        cur_time = datetime.now(tz=self.local_tz)
        cur_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date_utc = cur_time.astimezone(pytz.utc)
        end_date_utc = start_date_utc + timedelta(hours=24)
        ts_filter = {
            "$gte": start_date_utc,
            "$lte": end_date_utc
        }

        if resource_id == 1:
            #Get Baseline
            topic_path = 'target_agent/PNNL/350_BUILDING/goal/value'
            find_params = {
                'topic_id': ObjectId('597b5e70c56e526f28984f69'),
                'ts': ts_filter
            }

            data_cur = self.mongodb["data"].find(find_params).limit(1).sort([("ts", -1)])
            records = data_cur[:]
            for record in records:
                values = json.loads(record['value']['string_value'])

                for key, value in values.items():
                    # should check for timezone in meta instead of assuming this is local_tz
                    ts = datetime.fromtimestamp(float(key)/1000.0, pytz.utc)
                    ts = ts.astimezone(self.local_tz)
                    if ts.day < cur_time.day:
                        ts = ts + timedelta(hours=24)
                    ret_val.append({
                        'ts': format_ts(ts),
                        'value': value
                    })
        elif resource_id == 2:
            # Get target
            topic_path = 'target_agent/PNNL/350_BUILDING/goal/value'
            find_params = {
                'topic_id': ObjectId('5979dd11c56e526f28984f67'),
                'ts': ts_filter
            }

            data_cur = self.mongodb["data"].find(find_params).batch_size(2000)
            records = data_cur[:]

            for record in records:
                values = json.loads(record['value']['string_value'])
                start = parser.parse(values['start']).astimezone(tz=self.local_tz)
                end = parser.parse(values['end']).astimezone(tz=self.local_tz)
                ret_val.append({
                    'ts': format_ts(start),
                    #'end': format_ts(end),
                    #'cbp': values['cbp'],
                    'value': values['target']
                })

        elif resource_id == 3:
            # Get power
            topic_path = '350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/AverageBuildingPower'
            find_params = {
                #'topic_id': ObjectId('56de38d6c56e5232da276a54'),
                #'topic_id': ObjectId("56de38d6c56e5232da276a4f"),
                # 350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/AverageBuildingPower
                'topic_id': ObjectId("5978c6c5c56e526f28984f13"),
                'ts': ts_filter
            }
            data_cur = self.mongodb["data"].find(find_params).batch_size(10000)
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

        output = False
        if output:
            import csv
            keys = ret_val[0].keys()
            with open('output_' + str(resource_id) + '.csv', 'wb') as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writeheader()
                dict_writer.writerows(ret_val)

        return ret_val


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
        topic = request.args.get('topic')

        cur_time = datetime.now(tz=self.local_tz)
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
            ret_val.append({
                'ts': format_ts(record['ts']),
                'value': record['value']
            })

        if ret_val is not None:
            ret_val = sorted(ret_val, key=lambda k: k['ts'])

        return ret_val


class Root(Resource):
    def get(self):
        return current_app.send_static_file('index.html')

api.add_resource(Root, '/')
api.add_resource(PowerData, '/PowerData/<int:resource_id>')
api.add_resource(ZoneData, '/ZoneData')


def format_ts(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
