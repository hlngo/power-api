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
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'prod_historian'}
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'analysis'}
        mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}"
        mongo_uri = mongo_uri.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_default_database()
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
            #date param
            cur_time = request.args.get('date')
            if cur_time is None or cur_time == '0' or cur_time == 'undefined':
                cur_time = datetime.now(tz=self.local_tz)
            else:
                cur_time = self.local_tz.localize(parser.parse(cur_time))
            cur_time = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)

            # Hard code for the month of August
            cur_time = self.local_tz.localize(parser.parse("2018-08-01 00:00:00"))
            start_date_utc = cur_time.astimezone(pytz.utc)
            end_date_utc = start_date_utc + timedelta(hours=30 * 24)

            start_time = cur_time
            end_time = start_time + timedelta(hours=24)

            start_time = start_time.isoformat()
            end_time = end_time.isoformat()

            if resource_id == 1:
                #Get Baseline
                topic_path = 'PGnE/PNNL/350_BUILDING/baseline/value'

                # New from CSV
                with open('src/data/wbe350.csv') as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    for record in csv_reader:
                        ts = parser.parse(record[0])
                        ts = ts.replace(tzinfo=pytz.utc).astimezone(tz=self.local_tz) + timedelta(hours=7)
                        ret_val.append({
                            'ts': format_ts(ts),
                            'value': record[1],
                            'target': 160
                        })

            elif resource_id == 2:
                # Get target
                topic_path = 'target_agent/PNNL/350_BUILDING/goal/value'

            # Mongo
            elif resource_id == 3:
                topic = 'record/350-BUILDING ILC/PNNL/350_BUILDING/AverageBuildingPower'
                topic_id = ObjectId("59a09ed9c56e5232abb4e6d7")

                ts_filter = {
                    "$gte": start_date_utc,
                    "$lte": end_date_utc
                }

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

