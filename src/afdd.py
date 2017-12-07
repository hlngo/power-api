#TODO: logger and remove all raise exception
#TODO: renable energy impact

from flask import request
from flask_restful import Resource

import re
import json
import pytz
import traceback
import numpy as np
import scipy
import scipy.stats
import pandas as pd
from datetime import datetime, timedelta
from dateutil import parser

import pymongo
from bson.objectid import ObjectId

from utils import format_ts


prefix = 'record'
econ_rcx = 'EconomizerAIRCx' #'Economizer_RCx'
air_rcx = 'AirsideAIRCx' #'Airside_RCx'
update_topic_freq = 15  # mins
start_end_delta = 6  # days


class Afdd(Resource):
    def __init__(self):
        params = {'hostsandports': 'vc-db.pnl.gov', 'user': 'reader',
                  'passwd': 'volttronReader', 'database': 'analysis', 'auth_src': 'admin'}
        mongo_uri = "mongodb://{user}:{passwd}@{hostsandports}/{database}?authSource={auth_src}"
        mongo_uri = mongo_uri.format(**params)
        mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        self.mongodb = mongoclient.get_database(params['database'])
        #self.mongodb = mongoclient.get_default_database()

        self.local_tz = pytz.timezone('US/Pacific')
        self.delta_in_min = 60  # 15

        self.topics = {}
        self.topics_pull_time = None

    def query_topics(self):
        ret_val = []
        this_topics = {}

        if len(self.topics.keys()) == 0 \
            or self.topics_pull_time is None \
                or self.topics_pull_time + timedelta(minutes=update_topic_freq) < datetime.utcnow():
            regx = re.compile('^' + prefix + '.*AIRCx')
            data_cur = self.mongodb["topics"].find({"topic_name": regx})
            records = data_cur[:]
            for record in records:
                topic_id = record['_id']
                topic_name = record['topic_name']
                ret_val.append(topic_name)
                this_topics[topic_name] = topic_id

            if len(this_topics.keys()) > 0:
                self.topics_pull_time = datetime.utcnow()
                self.topics = this_topics
        else:
            ret_val = self.topics.keys()

        ret_val = {"result": ret_val}
        return ret_val

    def query_data(self,
                   site, building, unit,
                   dx,
                   start, end, aggr):
        points_available = {
            econ_rcx: ['diagnostic message', 'energy impact'],
            air_rcx: ['diagnostic message']
        }
        algo_set = {
            econ_rcx: [
                'Temperature Sensor Dx',
                'Not Economizing When Unit Should Dx',
                'Economizing When Unit Should Not Dx',
                'Excess Outdoor-air Intake Dx',
                'Insufficient Outdoor-air Intake Dx'
            ],
            air_rcx: [
                'Duct Static Pressure Set Point Control Loop Dx',
                'High Duct Static Pressure Dx',
                'High Supply-air Temperature Dx',
                'Low Duct Static Pressure Dx',
                'Low Supply-air Temperature Dx',
                'No Static Pressure Reset Dx',
                'No Supply-air Temperature Reset Dx',
                'Operational Schedule Dx',
                'Supply-air Temperature Set Point Control Loop Dx'
            ]
        }

        # Update topics before querying data
        self.query_topics()

        # Create bins
        topic_ids = []
        topic_bins = []  # Contains both message and energy impact
        base_topic = '/'.join([prefix, dx, site, building, unit])
        for algo in algo_set[dx]:
            for point in points_available[dx]:
                topic_name = '/'.join([base_topic, algo, point])
                if topic_name in self.topics:
                    topic_ids.append(self.topics[topic_name])
                    topic_bins.append({
                        'topic_name': topic_name,
                        'topic_id': self.topics[topic_name]
                    })
        ts_bins = []
        cur_date = start
        while cur_date <= end:
            new_bin = {'start': cur_date}
            cur_date = cur_date + timedelta(days=aggr)
            new_bin['end'] = cur_date - timedelta(seconds=1)
            ts_bins.append(new_bin)
        bins = []
        for topic_bin in topic_bins:
            for ts_bin in ts_bins:
                new_bin = {
                    'type': 1,  # dx message
                    'topic_name': topic_bin['topic_name'],
                    'topic_id': topic_bin['topic_id'],
                    'start': ts_bin['start'],
                    'end': ts_bin['end'],
                    'values': [],
                    'high': 0,
                    'low': 0,
                    'normal': 0
                }
                bins.append(new_bin)

        # Query data
        if len(topic_ids) == 0:
            return None

        ts_filter = {
            "$gte": start,
            "$lt": end
        }
        find_params = {
            'topic_id': {"$in": topic_ids},
            'ts': ts_filter
        }
        data_cur = self.mongodb["data"].find(find_params)  # .sort([("ts", -1)])

        # Put data into bins
        records = data_cur[:]
        for record in records:
            ts = record['ts']
            topic_id = record['topic_id']
            values = json.loads(record['value']['string_value'])  # high,normal,low
            ts = self.local_tz.localize(record['ts'])
            ts = ts.astimezone(pytz.utc)
            topic_id = record['topic_id']
            values = json.loads(record['value']['string_value'])  # high,normal,low
            bin = self.get_bin(bins, topic_id, ts)
            if bin is None:
                raise 'Cannot find bin ' + topic_id
            # if topic_id == ObjectId("59b2f6efc56e526d3856d5ca"):
            #     x = 1
            bin['values'].append(values)

        # Aggregation values in each bin
        for bin in bins:
            if bin['type'] == 1:  # dx message
                values = bin['values']
                if len(values) > 0:
                    high_fault, low_fault, normal_fault = 0, 0, 0
                    high_ok, low_ok, normal_ok = 0, 0, 0
                    code_high_fault, code_low_fault, code_normal_fault = -9999, -9999, -9999
                    code_high_ok, code_low_ok, code_normal_ok = -9999, -9999, -9999
                    for value in values:
                        # high fault-ok
                        code = round(float(value['high']), 1)
                        is_fault_code = self.is_fault(code)
                        if is_fault_code:
                            high_fault += 1
                            if code_high_fault == -9999:
                                code_high_fault = code
                        else:
                            high_ok += 1
                            if code_high_ok == -9999:
                                code_high_ok = code

                        # normal fault-ok
                        code = round(float(value['normal']), 1)
                        is_fault_code = self.is_fault(code)
                        if is_fault_code:
                            normal_fault += 1
                            if code_normal_fault == -9999:
                                code_normal_fault = code
                        else:
                            normal_ok += 1
                            if code_normal_ok == -9999:
                                code_normal_ok = code

                        # low fault-ok
                        code = round(float(value['low']), 1)
                        is_fault_code = self.is_fault(code)
                        if is_fault_code:
                            low_fault += 1
                            if code_low_fault == -9999:
                                code_low_fault = code
                        else:
                            low_ok += 1
                            if code_low_ok == -9999:
                                code_low_ok = code

                    # Aggregate this bin
                    bin['high'] = self.aggregate(high_fault, high_ok, code_high_fault, code_high_ok)
                    bin['normal'] = self.aggregate(normal_fault, normal_ok, code_normal_fault, code_normal_ok)
                    bin['low'] = self.aggregate(low_fault, low_ok, code_low_fault, code_low_ok)
                else: # no value => dx message = 0
                    pass
            else:  # energy impact
                values = bin['values']
                for value in values:
                    bin['high'] += round(float(value['high']), 1)
                    bin['normal'] += round(float(value['normal']), 1)
                    bin['low'] += round(float(value['low']), 1)

        # Produce output
        output = {'result': {
            'values': {}
        }}
        for bin in bins:
            bin_output = [
                format_ts(bin['start']),
                {'high': bin['high'], 'low': bin['low'], 'normal': bin['normal']}]
            if bin['topic_name'] not in output['result']['values']:
                output['result']['values'][bin['topic_name']] = []
            output['result']['values'][bin['topic_name']].append(bin_output)

        return output

        # Merge energy impact bin with dx message bin


    def aggregate(self, num_fault, num_no_fault, code_fault, code_ok, min_n=3):
        RED = code_fault
        GREEN = code_ok
        GREY = -1.2
        out_code = GREY

        p = 0.5
        conf = 0.95
        min_num_points = 5
        n = num_fault + num_no_fault
        if n < min_n:
            n = min_n

        x = scipy.linspace(0, n, n + 1)

        if x[-1:] == 0:
            raise "AfddAggrAgent: No Dx out"
        else:
            xmf_case_cdf = scipy.stats.binom.cdf(num_fault, n, p)
            intervals = scipy.stats.binom.interval(conf, n, p, loc=0)
            xmf_case1 = scipy.stats.binom.pmf(int(intervals[0]), n, p)

            if n > min_num_points:
                if xmf_case1 <= xmf_case_cdf:
                    out_code = RED
                else:
                    out_code = GREEN
            else:
                out_code = GREY

        return out_code

    def is_fault(self, code):
        # Round to tenths place.
        # Note: A decimal value of .1 indicates a RED color [fault]
        # Note: A decimal value of .2 indicates a GREY color [inconclusive] - except where noted above
        # Note: A decimal value of .0 indicates a GREEN color [no problems]
        # Note: A decimal value of .3 indicates a White color [no problems]
        is_fault = True
        state = (code * 10) % 10
        if code >= 0 and state == 1:  # RED
            is_fault = True
        elif code >= 0 and state == 0:  # GREEN
            is_fault = False

        return is_fault

    def get_bin(self, bins, topic_id, ts):
        for bin in bins:
            if bin['topic_id'] == topic_id and bin['start'] <= ts <= bin['end']:
                return bin
        return None

    def get(self, resource_id):
        ret_val = None

        try:
            if resource_id == 1:  # Query topics
                ret_val = self.query_topics()
            elif resource_id == 2:  # Query data
                site = request.args.get('site')
                building = request.args.get('building')
                unit = request.args.get('unit')
                dx = request.args.get('dx')
                aggr = request.args.get('aggr')
                if aggr is None or aggr == '':
                    aggr = 1
                else:
                    aggr = int(aggr)
                start = request.args.get('start')
                end = request.args.get('end')
                if start is None and end is None:
                    end_dt = datetime.now(tz=self.local_tz) + timedelta(days=1)
                    end_dt = end_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    start_dt = end_dt - timedelta(days=start_end_delta)
                elif start is None:
                    end_dt = self.local_tz.localize(parser.parse(end)) + timedelta(days=1)
                    start_dt = end_dt - timedelta(days=start_end_delta)
                elif end is None:
                    start_dt = self.local_tz.localize(parser.parse(start))
                    end_dt = start_dt + timedelta(days=start_end_delta)
                else:
                    start_dt = self.local_tz.localize(parser.parse(start))
                    end_dt = self.local_tz.localize(parser.parse(end))

                start_dt = start_dt.astimezone(pytz.utc)
                end_dt = end_dt.astimezone(pytz.utc)

                ret_val = self.query_data(site, building, unit, dx, start_dt, end_dt, aggr)
            output = False
            if output:
                with open('data.txt', 'w') as outfile:
                    json.dump(ret_val, outfile)
        except Exception, exc:
            traceback.print_exc()
            print(exc)
            return ret_val

        return ret_val


if __name__ == '__main__':
    afdd = Afdd()
    topics = afdd.query_topics()
    print(topics)
    econ = afdd.get(1)
    print(econ)
    air = afdd.get(2)
    print(air)
