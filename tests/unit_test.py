import json
import unittest

import os
import sys

sys.path.append(os.path.abspath('../stationdata'))
from stationdata import bqfuncs
from tests import send_data


def get_pvalue_msg(tag, elems):
    res = []
    for r in elems:
        if r.tag == tag:
            res.append(r.value)
    return res
        
class StationDataTest(unittest.TestCase):
    def test_check_invalid_messages(self):
        expected_msgs = [{
            "_version": "3.0.1",
            "device_id": "unit-test",
            "data": [
                send_data.broken_msgs["data"][1],
                send_data.broken_msgs["data"][2],
                send_data.broken_msgs["data"][3],
            ]
        }]
        msgs = bqfuncs.unpack_messages(send_data.broken_msgs)
        filtered_msgs = get_pvalue_msg(bqfuncs.INVALID_DATA, msgs)
        self.assertEqual(
            filtered_msgs,
            expected_msgs,
        )

        filtered_msgs = get_pvalue_msg(bqfuncs.WORKOUT_DATA, msgs)
        self.assertEqual(
            filtered_msgs,
            [send_data.broken_msgs["data"][0]],
        )

    def test_check_valid_messages(self):
        expected_fw = [{'timestamp': 1595336369, 'user_id': 'auth0|asdfasfds', 'plan_workout_id': 123, 'plan_set_id': 0, 'history_workout_id': 'UUID', 'type': 'Notification', 'sub_type': 'HeightButton', 'payload': '{"Notification": {"HeightButton": {"Side": "LEFT", "Status": "PRESS"}}}', 'device_id': 'unit-test'}]
        expected_hr = [{'timestamp': 1595336369, 'user_id': 'auth0|asdfasfds', 'plan_workout_id': 123, 'plan_set_id': 0, 'history_workout_id': '4797f901-929d-4b4c-b2b4-29a5aa4edaa4', 'history_set_id': 'UUID', 'bpm': 76, 'monitor_device_id': '00:00:00', 'monitor_device_name': 'FORME DEVELOPMENT', 'device_id': 'unit-test'}]

        msgs = bqfuncs.unpack_messages(send_data.full_sample)
        filtered_msgs = get_pvalue_msg(bqfuncs.FIRMWARE_DATA, msgs)
        self.assertEqual(
            filtered_msgs,
            expected_fw,
        )

        filtered_msgs = get_pvalue_msg(bqfuncs.HEART_RATE_DATA, msgs)
        self.assertEqual(
            filtered_msgs,
            expected_hr,
        )

    def test_get_schema(self):
        res = bqfuncs.get_schema("firmwareData")
        self.assertEqual(
            res,
            'timestamp:timestamp,device_id:string,user_id:string,plan_workout_id:integer,plan_set_id:integer,history_workout_id:string,history_set_id:string,history_rep_id:string,message_id:integer,payload:string,type:string,sub_type:string',
        )