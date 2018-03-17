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

        # Mongo 2.8 to fix multihost issue
        mongoclient = pymongo.MongoClient(mongo_uri)
        self.mongodb = mongoclient.get_default_database()

        # Mong 3.5 issue with using multihost
        # mongoclient = pymongo.MongoClient(mongo_uri, connect=False)
        # self.mongodb = mongoclient.get_database(params['database'])

        self.local_tz = pytz.timezone('US/Pacific')
        self.delta_in_min = 60  # 15

        self.topics = {}
        self.topics_pull_time = None
        self.points_available = {
            econ_rcx: ['diagnostic message', 'energy impact'],
            air_rcx: ['diagnostic message']
        }
        self.algo_set = {
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

    def query_data_to_bin(self,
                          site, building, unit,
                          dx,
                          start, end, aggr, save_raw=False):
        # aggr is in hour

        # Update topics before querying data
        self.query_topics()

        # Create bins
        topic_ids = []
        topic_bins = []  # Contains both message and energy impact
        base_topic = '/'.join([prefix, dx, site, building, unit])
        for algo in self.algo_set[dx]:
            for point in self.points_available[dx]:
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
            cur_date = cur_date + timedelta(hours=aggr)
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
            ts = pytz.utc.localize(ts)
            topic_id = record['topic_id']
            values = json.loads(record['value']['string_value'])  # high,normal,low
            #ts = self.local_tz.localize(record['ts'])
            #ts = ts.astimezone(pytz.utc)
            bin = self.get_bin(bins, topic_id, ts)
            if bin is None:
                raise 'Cannot find bin ' + topic_id
            # if topic_id == ObjectId("59b2f6efc56e526d3856d5ca"):
            #     x = 1
            bin['values'].append(values)

        # Save raw data
        if save_raw:
            name = "./data/%s_%s_%s.csv" % (building, unit, dx)
            content = 'ts,topic_prefix,app,site,building,unit,dx,dx_code,topic_id,high,low,normal\n'
            for bin in bins:
                for value in bin['values']:
                    content += ','.join([bin['start'].strftime('%Y-%m-%d %H:%M:%S'),
                                         bin['topic_name'].replace('/', ','),
                                         str(bin['topic_id']),
                                         str(value['high']),
                                         str(value['low']),
                                         str(value['normal']),
                                         '\n'])
            with open(name, 'w') as file:
                file.write(content)

        return bins

    def query_data(self, site, building, unit, dx, start, end, aggr, save_raw=False):
        bins = self.query_data_to_bin(site, building, unit, dx, start, end, 1, save_raw)

        if save_raw:
            return

        # Produce output
        output = {'result': {
            'values': {}
        }}
        for bin in bins:
            values = bin['values']
            if len(values) > 0:
                high = low = normal = -9999
                for value in values:
                    high_postfix = self.get_fault_postfix(high)
                    low_postfix = self.get_fault_postfix(low)
                    normal_postfix = self.get_fault_postfix(normal)

                    # high fault
                    code = round(float(value['high']), 1)
                    postfix = self.get_fault_postfix(code)
                    if high == -9999:
                        high = code
                    elif postfix == 1:  # RED
                        high = code
                    elif postfix == 2 and high_postfix != 1:  # GREY and current code is not RED
                        high = code
                    elif postfix == 0 and high_postfix == 3:  # GREEN and current code is not WHITE
                        high = code

                    # low fault
                    code = round(float(value['low']), 1)
                    postfix = self.get_fault_postfix(code)
                    if low == -9999:
                        low = code
                    elif postfix == 1:  # RED
                        low = code
                    elif postfix == 2 and low_postfix != 1:  # GREY and current code is not RED
                        low = code
                    elif postfix == 0 and low_postfix == 3:  # GREEN and current code is not WHITE
                        low = code

                    # normal fault
                    code = round(float(value['normal']), 1)
                    postfix = self.get_fault_postfix(code)
                    if normal == -9999:
                        normal = code
                    elif postfix == 1:  # RED
                        normal = code
                    elif postfix == 2 and normal_postfix != 1:  # GREY and current code is not RED
                        normal = code
                    elif postfix == 0 and normal_postfix == 3:  # GREEN and current code is not WHITE
                        normal = code

                bin_output = [
                    # Return data with local timezone where Dx occurs
                    format_ts(bin['end'].astimezone(self.local_tz)),
                    {'high': high, 'low': low, 'normal': normal}
                ]
                if bin['topic_name'] not in output['result']['values']:
                    output['result']['values'][bin['topic_name']] = []

                output['result']['values'][bin['topic_name']].append(bin_output)

        return output

    def query_aggr_data(self, site, building, unit, dx, start, end, aggr, save_raw=False):

        bins = self.query_data_to_bin(site, building, unit, dx, start, end, aggr*24, save_raw)

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
                        postfix = self.get_fault_postfix(code)
                        if postfix == 1:
                            high_fault += 1
                            if code_high_fault == -9999:
                                code_high_fault = code
                        elif postfix == 0:
                            high_ok += 1
                            if code_high_ok == -9999:
                                code_high_ok = code

                        # normal fault-ok
                        code = round(float(value['normal']), 1)
                        is_fault_code = self.get_fault_postfix(code)
                        if postfix == 1:
                            normal_fault += 1
                            if code_normal_fault == -9999:
                                code_normal_fault = code
                        elif postfix == 0:
                            normal_ok += 1
                            if code_normal_ok == -9999:
                                code_normal_ok = code

                        # low fault-ok
                        code = round(float(value['low']), 1)
                        is_fault_code = self.get_fault_postfix(code)
                        if postfix == 1:
                            low_fault += 1
                            if code_low_fault == -9999:
                                code_low_fault = code
                        elif postfix == 0:
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
                format_ts(bin['end'].astimezone(self.local_tz)),
                {'high': bin['high'], 'low': bin['low'], 'normal': bin['normal']}
            ]
            if bin['topic_name'] not in output['result']['values']:
                output['result']['values'][bin['topic_name']] = []

            output['result']['values'][bin['topic_name']].append(bin_output)

        return output

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

    def get_fault_postfix(self, code):
        # Round to tenths place.
        # Note: A decimal value of .1 indicates a RED color [fault]
        # Note: A decimal value of .2 indicates a GREY color [inconclusive] - except where noted above
        # Note: A decimal value of .0 indicates a GREEN color [no problems]
        # Note: A decimal value of .3 indicates a White color [no problems]
        post_fix = -9999
        if code < 0:
            code *= -1.0

        return (code * 10) % 10

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
            elif resource_id == 2 or resource_id == 3:  # Query data
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
                if resource_id == 2:
                    ret_val = self.query_aggr_data(site, building, unit, dx, start_dt, end_dt, aggr)
                if resource_id == 3:
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

    site = 'PNNL'
    building = 'EMSL'
    #units = ['AHU6', 'AHU7','AHU8','AHU13','AHU20']
    units = ['AHU6']
    #dxs = ['EconomizerAIRCx'] #'AirsideAIRCx',
    dxs = ['EconomizerAIRCx', 'AirsideAIRCx']
    local_tz = pytz.timezone('US/Pacific')
    start_dt = local_tz.localize(parser.parse('2017-09-30 00:00:00'))
    end_dt = local_tz.localize(parser.parse('2018-01-01 12:00:00'))
    aggr = 1  # hours
    resource_id = 3

    for unit in units:
        for dx in dxs:
            if resource_id == 2:
                ret_val = afdd.query_aggr_data(site, building, unit, dx, start_dt, end_dt, aggr, save_raw=False)
            if resource_id == 3:
                ret_val = afdd.query_data(site, building, unit, dx, start_dt, end_dt, aggr, save_raw=False)



