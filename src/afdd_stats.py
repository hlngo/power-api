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
import os
import pymongo
from bson.objectid import ObjectId

from utils import format_ts


prefix = 'record'
econ_rcx = 'EconomizerAIRCx' #'Economizer_RCx'
air_rcx = 'AirsideAIRCx' #'Airside_RCx'
update_topic_freq = 15  # mins
start_end_delta = 6  # days


class Afdd():
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
                   site, building,
                   dx,
                   start, end):
        points_available = {
            econ_rcx: ['diagnostic message'],
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
        for topic in self.topics.keys():
            if site in topic and building in topic and dx in topic:
                topic_name = topic
                topic_ids.append(self.topics[topic_name])
                topic_bins.append({
                    'topic_name': topic_name,
                    'topic_id': self.topics[topic_name]
                })

        bins = []
        for topic_bin in topic_bins:
            new_bin = {
                'type': 1,  # dx message
                'topic_name': topic_bin['topic_name'],
                'topic_id': topic_bin['topic_id'],
                'values': [],
                'high_fault': 0,
                'high_ok': 0,
                'low_fault': 0,
                'low_ok': 0,
                'normal_fault': 0,
                'normal_ok': 0
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
            bin['values'].append(values)

        # Aggregation values in each bin
        for bin in bins:
            if bin['type'] == 1:  # dx message
                values = bin['values']
                if len(values) > 0:
                    high_fault, low_fault, normal_fault = 0, 0, 0
                    high_ok, low_ok, normal_ok = 0, 0, 0
                    for value in values:
                        code = round(float(value['high']), 1)
                        high_fault, high_ok = self.count_fault_ok(code, high_fault, high_ok)
                        code = round(float(value['normal']), 1)
                        normal_fault, normal_ok = self.count_fault_ok(code, normal_fault, normal_ok)
                        code = round(float(value['low']), 1)
                        low_fault, low_ok = self.count_fault_ok(code, low_fault, low_ok)
                    # Aggregate this bin
                    bin['high_fault'] = high_fault
                    bin['high_ok'] = high_ok
                    bin['normal_fault'] = normal_fault
                    bin['normal_ok'] = normal_ok
                    bin['low_fault'] = low_fault
                    bin['low_ok'] = low_ok
                else: # no value => dx message = 0
                    pass
            else:  # energy impact
                pass

        # Produce output
        output = {
        }
        for bin in bins:
            output[bin['topic_name']] = {
                'high_fault': bin['high_fault'],
                'low_fault': bin['low_fault'],
                'normal_fault': bin['normal_fault'],
                'high_ok': bin['high_ok'],
                'low_ok': bin['low_ok'],
                'normal_ok': bin['normal_ok']
            }

        return output

    def get_bin(self, bins, topic_id, ts):
        for bin in bins:
            if bin['topic_id'] == topic_id:
                return bin
        return None

    def count_fault_ok(self, code, fault, ok):
        # Round to tenths place.
        # Note: A decimal value of .1 indicates a RED color [fault]
        # Note: A decimal value of .2 indicates a GREY color [inconclusive] - except where noted above
        # Note: A decimal value of .0 indicates a GREEN color [no problems]
        # Note: A decimal value of .3 indicates a White color [no problems]
        state = (code * 10) % 10
        if code >= 0 and state == 1:  # RED
            fault += 1
        elif code >= 0 and state == 0:  # GREEN
            ok += 1

        return fault, ok


def get_summary(output, building, rcx):
    print("Extracting stats for building " + building + " " + rcx)
    high_fault = low_fault = normal_fault = 0
    high_ok = low_ok = normal_ok = 0

    for value in output.values():
        high_fault += value['high_fault']
        low_fault += value['low_fault']
        normal_fault += value['normal_fault']
        high_ok += value['high_ok']
        low_ok += value['low_ok']
        normal_ok += value['normal_ok']

    high_fr = float(high_fault)/(high_fault+high_ok) if (high_fault+high_ok)>0 else 0
    low_fr = float(low_fault)/(low_fault + low_ok) if (low_fault+low_ok)>0 else 0
    normal_fr = float(normal_fault)/(normal_fault + normal_ok) if (normal_fault+normal_ok)>0 else 0

    print("(high) Building {b} {rcx} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building, rcx=rcx,
        f=high_fault, ok=high_ok, fr=high_fr
    ))
    print("(normal) Building {b} {rcx} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building, rcx=rcx,
        f=normal_fault, ok=normal_ok, fr=normal_fr
    ))
    print("(low) Building {b} {rcx} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building, rcx=rcx,
        f=low_fault, ok=low_ok, fr=low_fr
    ))
    print(os.linesep)

    return high_fault, high_ok, normal_fault, normal_ok, low_fault, low_ok


def init_result(res, building):
    if building not in res:
        res[building] = {}
        res[building]['high_fault'] = 0
        res[building]['low_fault'] = 0
        res[building]['normal_fault'] = 0
        res[building]['high_ok'] = 0
        res[building]['low_ok'] = 0
        res[building]['normal_ok'] = 0


def get_building_summary(res, building):
    high_fault = low_fault = normal_fault = 0
    high_ok = low_ok = normal_ok = 0
    value = res[building]
    high_fault += value['high_fault']
    low_fault += value['low_fault']
    normal_fault += value['normal_fault']
    high_ok += value['high_ok']
    low_ok += value['low_ok']
    normal_ok += value['normal_ok']

    high_fr = float(high_fault) / (high_fault + high_ok) if (high_fault + high_ok) > 0 else 0
    low_fr = float(low_fault) / (low_fault + low_ok) if (low_fault + low_ok) > 0 else 0
    normal_fr = float(normal_fault) / (normal_fault + normal_ok) if (normal_fault + normal_ok) > 0 else 0

    print("(high) Building {b} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building,
        f=high_fault, ok=high_ok, fr=high_fr
    ))
    print("(normal) Building {b} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building,
        f=normal_fault, ok=normal_ok, fr=normal_fr
    ))
    print("(low) Building {b} stats: fault={f} good={ok} fault_fraction={fr}".format(
        b=building,
        f=low_fault, ok=low_ok, fr=low_fr
    ))
    print(os.linesep)


def get_all_buildings_summary(res, buildings):
    high_fault = low_fault = normal_fault = 0
    high_ok = low_ok = normal_ok = 0
    for building in buildings:
        value = res[building]
        high_fault += value['high_fault']
        low_fault += value['low_fault']
        normal_fault += value['normal_fault']
        high_ok += value['high_ok']
        low_ok += value['low_ok']
        normal_ok += value['normal_ok']

    high_fr = float(high_fault) / (high_fault + high_ok) if (high_fault + high_ok) > 0 else 0
    low_fr = float(low_fault) / (low_fault + low_ok) if (low_fault + low_ok) > 0 else 0
    normal_fr = float(normal_fault) / (normal_fault + normal_ok) if (normal_fault + normal_ok) > 0 else 0

    print("(high) All buildings' stats: fault={f} good={ok} fault_fraction={fr}".format(
        f=high_fault, ok=high_ok, fr=high_fr
    ))
    print("(normal) All buildings' stats: fault={f} good={ok} fault_fraction={fr}".format(
        f=normal_fault, ok=normal_ok, fr=normal_fr
    ))
    print("(low) All buildings' stats: fault={f} good={ok} fault_fraction={fr}".format(
        f=low_fault, ok=low_ok, fr=low_fr
    ))
    print(os.linesep)


if __name__ == '__main__':
    afdd = Afdd()
    end = datetime.utcnow()
    res = {}

    # Get result for last 60 days
    start = end - timedelta(days=5)
    rcxs = [econ_rcx, air_rcx]
    site = 'PNNL'
    buildings = ['SEB', '350_BUILDING', 'EMSL', 'BSF_CSF', '3860_BUILDING', 'ROI', 'ROII']
    for building in buildings:
        init_result(res, building)
    for building in buildings:
        for rcx in rcxs:
            output = afdd.query_data(site, building, rcx, start, end)
            if output is None:
                print("(high) Building {b} {rcx} stats: No result".format(b=building, rcx=rcx))
                continue
            high_fault, high_ok, normal_fault, normal_ok, low_fault, low_ok = get_summary(output, building, rcx)
            res[building]['high_fault'] += high_fault
            res[building]['low_fault'] += low_fault
            res[building]['normal_fault'] += normal_fault
            res[building]['high_ok'] += high_ok
            res[building]['low_ok'] += low_ok
            res[building]['normal_ok'] += normal_ok
        get_building_summary(res, building)

    site2 = 'PNNL-SEQUIM'
    buildings2 = ['MSL5']
    for building in buildings2:
        init_result(res, building)
    for building in buildings2:
        for rcx in rcxs:
            output = afdd.query_data(site2, building, rcx, start, end)
            if output is None:
                print("(high) Building {b} {rcx} stats: No result".format(b=building, rcx=rcx))
                continue
            high_fault, high_ok, normal_fault, normal_ok, low_fault, low_ok = get_summary(output, building, rcx)
            res[building]['high_fault'] += high_fault
            res[building]['low_fault'] += low_fault
            res[building]['normal_fault'] += normal_fault
            res[building]['high_ok'] += high_ok
            res[building]['low_ok'] += low_ok
            res[building]['normal_ok'] += normal_ok
        get_building_summary(res, building)

    #All buildings
    buildings.extend(buildings2)
    get_all_buildings_summary(res, buildings)
