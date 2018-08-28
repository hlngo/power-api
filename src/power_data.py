from flask import request
from flask_restful import Resource

import csv
import json
import pytz
import traceback
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser

import pymongo
from bson.objectid import ObjectId

from utils import format_ts
from crate import client


class PowerData(Resource):
    def __init__(self):
        # Mongo
        data_params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': '2017_production'}
        prod_data_params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                       'passwd': 'volttronReader', 'database': 'prod_historian'}

        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'analysis'}

        mongo_uri_tmpl = "mongodb://{user}:{passwd}@{hostsandports}/{database}"

        mongo_uri = mongo_uri_tmpl.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_default_database()

        data_mongo_uri = mongo_uri_tmpl.format(**data_params)
        data_mongoclient = pymongo.MongoClient(data_mongo_uri, connect=False)
        self.data_mongodb = data_mongoclient.get_default_database()

        prod_data_mongo_uri = mongo_uri_tmpl.format(**prod_data_params)
        prod_data_mongoclient = pymongo.MongoClient(prod_data_mongo_uri, connect=False)
        self.prod_data_mongodb = prod_data_mongoclient.get_default_database()

        # Crate
        self.host = "http://172.18.65.54:4200"
        self.connection = client.connect(self.host)

        self.local_tz = pytz.timezone('US/Pacific')
        self.delta_in_min = 60 #15

        # Cooling
        # Baseline date: 8 / 5 / 2016
        # ILC date: 8 / 16 / 2016

        # Heating
        # Baseline date: 3 / 14 / 2016
        # ILC date: 3 / 15 / 2016

    def get(self, resource_id):
        ret_val = []

        try:
            # date param
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
            # func param
            func = request.args.get('func')
            if func is None or func == '0':
                func = '30min'

            # season param
            season = request.args.get('season')
            if season == 'cooling' or season == 'Cooling':
                season_base_date = '2016-08-05'
                season_actual_date = '2016-08-16'
            else:
                season_base_date = '2016-03-14'
                season_actual_date = '2016-03-15'

            if resource_id == 1:
                # Get Baseline
                topic_path = 'PGnE/PNNL/350_BUILDING/baseline/value'
                topic_id = ObjectId("597b5ecfc56e523f5792d40d")  # ObjectId('597b5e70c56e526f28984f69')
                find_params = {
                    'topic_id': topic_id,
                    'ts': ts_filter
                }

                data_cur = self.data_mongodb["data"].find(find_params).sort([("ts", pymongo.DESCENDING)]).batch_size(10000)
                records = data_cur[:]
                for record in records.limit(1):
                    values = json.loads(record['value']['string_value'])

                    for key, value in values.items():
                        # should check for timezone in meta instead of assuming this is local_tz
                        ts = datetime.fromtimestamp(float(key) / 1000.0, pytz.utc)
                        ts = ts.astimezone(self.local_tz)
                        # circular shift 1-hour
                        # ts = ts + timedelta(hours=1)
                        ts = ts + timedelta(minutes=self.delta_in_min)

                        #
                        # This section below needs to be revised carefully if we want to show more than a day
                        # align to current day
                        if ts.day != cur_time.day:
                            ts = ts.replace(year=cur_time.year, month=cur_time.month, day=cur_time.day)

                        #
                        ret_val.append({
                            'ts': format_ts(ts),
                            'value': value
                        })
            elif resource_id == 2:
                # Get target
                topic_path = 'target_agent/PNNL/350_BUILDING/goal/value'
                topic_id = ObjectId("5979dcdcc56e523f5792d40b")  #ObjectId('5979dd11c56e526f28984f67')
                find_params = {
                    'topic_id': topic_id,
                    'ts': ts_filter
                }

                data_cur = self.data_mongodb["data"].find(find_params).batch_size(2000)
                records = data_cur[:]

                for record in records:
                    values = json.loads(record['value']['string_value'])
                    start = parser.parse(values['start']).astimezone(tz=self.local_tz)
                    end = parser.parse(values['end']).astimezone(tz=self.local_tz)

                    # circular shift 1-hour
                    # start = start + timedelta(hours=1)
                    # end = end + timedelta(hours=1)

                    ts = start
                    if start < datetime(2017, 8, 7, 0, 0, tzinfo=self.local_tz):
                        start = start + timedelta(minutes=self.delta_in_min)
                        end = end + timedelta(minutes=self.delta_in_min)
                        ts = end

                    ret_val.append({
                        'ts': format_ts(ts),
                        # 'ts': format_ts(start),
                        # 'ts': format_ts(end),
                        # 'cbp': values['cbp'],
                        'value': values['target']
                    })

            elif resource_id == 3:
                # Get power
                # topic_path = '350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/AverageBuildingPower'
                topic_id = ObjectId("5978c751c56e523f5792d3b7")  # topic_id = ObjectId("5978c6c5c56e526f28984f13")
                hourly = False
                if func == '30min' or func == '30m':
                    # 350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/AverageBuildingPower
                    topic_id = ObjectId("5978c6c5c56e526f28984f13")
                elif func == '60min' or func == '60m' or func == '1h' or func == '1hour' or func == 'hourly':
                    # PNNL/350_BUILDING/METERS/WholeBuildingPowerWithoutShopAirCompressor
                    topic_id = ObjectId("5935b8ddc56e523f5792c187")  #topic_id = ObjectId("56de38d6c56e5232da276a51")
                    hourly = True
                elif func == 'exp' or func == 'exponential':
                    # 350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/LoadControlPower
                    topic_id = ObjectId("5978c751c56e523f5792d3b9")  # topic_id = ObjectId("5978c6c5c56e526f28984f15")
                elif func == 'realtime':
                    # PNNL/350_BUILDING/METERS/WholeBuildingPowerWithoutShopAirCompressor
                    topic_id = ObjectId("5935b8ddc56e523f5792c187")  # topic_id = ObjectId("56de38d6c56e5232da276a51")

                find_params = {
                    # 350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower/AverageBuildingPower
                    # 'topic_id': ObjectId("5978c6c5c56e526f28984f13"),
                    # PNNL / 350_BUILDING / METERS / WholeBuildingPowerWithoutShopAirCompressor
                    # 'topic_id': ObjectId("56de38d6c56e5232da276a51"),
                    # PNNL / 350_BUILDING / METERS / WholeBuildingPower
                    # 'topic_id': ObjectId("56de38d6c56e5232da276a54"),
                    # 'topic_id': ObjectId("5978c6c5c56e526f28984f15"),
                    'topic_id': topic_id,
                    'ts': ts_filter
                }
                data_cur = self.prod_data_mongodb["data"].find(find_params).batch_size(10000)
                records = data_cur[:]

                if hourly:
                    cur_hr = -1
                    cur_hr_values = []
                    for record in records:
                        ts = record['ts']
                        ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                        value = float(record['value'])
                        ret_val.append({
                            'ts': ts,
                            'value': value
                        })

                    df = pd.DataFrame(ret_val)
                    df = df.set_index(['ts'])
                    # df = df.groupby([df.index.year, df.index.month, df.index.day, df.index.hour])['value'].mean()
                    df = df.resample('1H', closed='right', label='left').mean()
                    df = df.reset_index(level=['ts'])
                    ret_val = df.T.to_dict().values()
                    ret_val = [{
                        'ts': format_ts(item['ts'] + timedelta(minutes=self.delta_in_min)),
                        'value': item['value']} for item in ret_val]
                else:
                    for record in records:
                        ts = record['ts']
                        ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                        ret_val.append({
                            'ts': format_ts(ts),
                            'value': record['value']
                        })

            elif resource_id == 4 or resource_id == 5:
                # PNNL / 350_BUILDING / METERS / WholeBuildingPowerWithoutShopAirCompressor
                topic_id = ObjectId("56de38d6c56e5232da276a51")
                cur_time = season_base_date
                if resource_id == 5:
                    cur_time = season_actual_date

                cur_time = self.local_tz.localize(parser.parse(cur_time))
                cur_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
                start_date_utc = cur_time.astimezone(pytz.utc) + timedelta(minutes=30)
                end_date_utc = start_date_utc + timedelta(hours=24) + timedelta(minutes=30)
                ts_filter = {
                    "$gte": start_date_utc,
                    "$lte": end_date_utc
                }

                find_params = {
                    'topic_id': topic_id,
                    'ts': ts_filter
                }
                data_cur = self.data_mongodb["data"].find(find_params).batch_size(10000)
                records = data_cur[:]

                for record in records:
                    ts = record['ts']
                    ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                    value = float(record['value'])
                    ret_val.append({
                        'ts': ts,
                        'value': value
                    })

                df = pd.DataFrame(ret_val)
                df = df.set_index(['ts'])
                df['demand'] = df['value'].rolling(min_periods=30, window=30, center=False).mean()
                df = df.dropna()
                df = df.reset_index(level=['ts'])
                ret_val = df.T.to_dict().values()
                ret_val = [{
                    'ts': format_ts(item['ts'].replace(year=2017, month=8, day=10)),
                    'value': item['demand']} for item in ret_val]

            elif resource_id == 10:
                topic = 'record/350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower'
                topic_id = ObjectId("59a09ed9c56e5232abb4e6d7")

                # Hard code for the month of August
                cur_time = self.local_tz.localize(parser.parse("2018-08-01 00:00:00"))
                start_date_utc = cur_time.astimezone(pytz.utc)
                end_date_utc = start_date_utc + timedelta(hours=30 * 24)

                ts_filter = {
                    "$gte": start_date_utc,
                    "$lte": end_date_utc
                }

                find_params = {
                    'topic_id': topic_id,
                    'ts': ts_filter
                }
                data_cur = self.mongodb["data"].find(find_params).batch_size(10000)
                records = data_cur[:]

                for record in records:
                    ts = record['ts']
                    ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
                    ret_val.append({
                        'ts': format_ts(ts),
                        'value': record['value'][0]['AverageBuildingPower'],
                        'target': 160
                    })

            # Crate
            # elif resource_id == 3:
            #     # Get power
            #     topic = 'PNNL/350_BUILDING/METERS/WholeBuildingPowerWithoutShopAirCompressor'
            #     #start_time = '2018-08-01T00:00:00-07:00'  # US/Pacific time
            #     #end_time = '2018-08-29T08:00:00-07:00'  # US/Pacific time
            #     limit = 100000
            #
            #     query = "SELECT ts, double_value " \
            #             "FROM cetc_infrastructure.data " \
            #             "WHERE topic = '{topic}' " \
            #             "AND ts BETWEEN '{start}' AND '{end}' " \
            #             "ORDER BY ts " \
            #             "LIMIT {limit};".format(topic=topic,
            #                                     start=start_time,
            #                                     end=end_time,
            #                                     limit=limit)
            #
            #     cur = self.connection.cursor()
            #     cur.execute(query)
            #     records = cur.fetchall()
            #
            #     for record in records:
            #         ts = datetime.fromtimestamp(int(record[0])/1000)
            #         ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz)
            #         ret_val.append({
            #             'ts': format_ts(ts),
            #             'value': record[1],
            #             'target': 160,
            #             'baseline': record[1]-5
            #         })

            if ret_val is not None:
                ret_val = sorted(ret_val, key=lambda k: k['ts'])

            output = False
            # Remember the baseline is modified before it so it is not the raw data anymore
            if output:
                keys = ret_val[0].keys()
                with open('output_' + str(resource_id) + '.csv', 'wb') as output_file:
                    dict_writer = csv.DictWriter(output_file, keys)
                    dict_writer.writeheader()
                    dict_writer.writerows(ret_val)
        except Exception, exc:
            traceback.print_exc()
            print(exc)
            return ret_val

        return ret_val

