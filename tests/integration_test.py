
import sys

from google.cloud import pubsub_v1

import logging
import os
import re
import tempfile
import unittest
import json
import multiprocessing
import subprocess
import time

from random import randint

import send_data
sys.path.append(os.path.abspath('../stationdata'))
from stationdata import bqfuncs
from stationdata import run

from apache_beam.options import pipeline_options

# from apache_beam.testing.util import open_shards

class TestOptions(pipeline_options.TestDataflowOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        run.required_parameters(parser)
        parser.add_argument(
            '--project',
            required=True,
            help=('Run dataflow locally (direct) or in the cloud (dataflow)'))

class StationDataTest(unittest.TestCase):
    def setUp(self):
        self.cfg = TestOptions([
            "--runner", "DirectRunner",
            "--project", os.environ["PROJECT_ID"],
            "--iot_topic_telemetry", os.environ["IOT_TOPIC_TELEMETRY"],
            "--iot_topic_dead_letter", os.environ["IOT_TOPIC_DEAD_LETTER"],
            "--cloud_topic_workout", os.environ["CLOUD_TOPIC_WORKOUT"],
            "--bq_data_set", os.environ["BIGQUERY_DATASET"],
            "--bq_firmware", '{}{}{}'.format(
                os.environ["BIGQUERY_FIRMWARE_TABLE"],
                randint(0, 9),
                randint(0, 9),
            ),
            "--bq_heart_rate", '{}{}{}'.format(
                os.environ["BIGQUERY_HEARTRATE_TABLE"],
                randint(0, 9),
                randint(0, 9),
            ),
            "--job_name", os.environ["JOB_NAME"],
            "--region", os.environ["REGION"],
        ])
        self.publisher = pubsub_v1.PublisherClient()

    def tearDown(self):
        self.dataflowThread.terminate()
        # remove firmware data temp table
        os.system("bq rm -f -t {}.{}".format(
          self.cfg.bq_data_set,
          self.cfg.bq_firmware,
        ))
        # remove heartrate temp table
        os.system("bq rm -f -t {}.{}".format(
          self.cfg.bq_data_set,
          self.cfg.bq_heart_rate,
        ))

    def start_dataflow(self):

        # remove table from previous unsuccess run
        # remove firmware data temp table
        os.system("bq rm -f -t {}.{}".format(
          self.cfg.bq_data_set,
          self.cfg.bq_firmware,
        ))
        # remove heartrate temp table
        os.system("bq rm -f -t {}.{}".format(
          self.cfg.bq_data_set,
          self.cfg.bq_heart_rate,
        ))

        self.dataflowThread = multiprocessing.Process(
            target=run.start_dataflow,
            args=(self.cfg,),
        )
        self.dataflowThread.start()

    def test_integration_test(self):
        """
        test function:
        1. start dataflow process
        2. run sendData function
        3. pause for 45 seconds?
        4. check if bigquery has the data (firmware and heartrate)
        5. check if we have data in workout pubsub
        6. remove big query table
        """

        self.start_dataflow()
        time.sleep(3)

        messages = []
        dead_letters = []
        self.listen_workout_data(messages)
        self.listen_dead_letter(dead_letters)
        # ToDo
        # Test that we have data in bigquery
        send_data.publish(send_data.full_sample)
        send_data.publish(send_data.broken_msgs)
        send_data.publish(send_data.full_sample)
        sleep_time = 45
        print("going to sleep for {} seconds".format(sleep_time))
        time.sleep(sleep_time)
        self.assertEqual(len(messages), 3, "wrong amount of pubsub messages {}".format(messages))

        # ToDo
        # Don’t SELECT * to preview data. Use bq head or tabledata.list instead.
        bigQuerySelect = 'bq query -q --format=json "SELECT * FROM {}.{}"'.format(
            self.cfg.bq_data_set,
            self.cfg.bq_firmware,
        )
        res = self.check_bigquery(bigQuerySelect)
        self.assertEqual(len(res), 2, "wrong amount of firmware messages {}".format(res))

        # Test Heart Rate
        bigQuerySelect = 'bq query -q --format=json "SELECT * FROM {}.{}"'.format(
            self.cfg.bq_data_set,
            self.cfg.bq_heart_rate,
        )
        res = self.check_bigquery(bigQuerySelect)
        self.assertEqual(len(res), 2, "heart rate error {}".format(res))

        self.assertEqual(len(dead_letters), 1, "wrong amount of dead letters {}".format(dead_letters))
        # ToDo
        # Verify dead letters content

    def check_bigquery(self, bigQuerySelect):
        json_res = subprocess.check_output(
            bigQuerySelect,
            shell=True,
            stderr=sys.stdout,
        )

        res = json.loads(json_res)
        return res

    def listen_workout_data(self, message_buffer):
        subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_id}`
        subscription_path = subscriber.subscription_path(
            "formelife-dev",
            "{}-history".format(os.environ["CLOUD_TOPIC_WORKOUT"]),
        )

        def callback(message):
            message_buffer.append(message)
            # print("Received message: {}".format(message))
            message.ack()

        subscriber.subscribe(subscription_path, callback=callback)

    def listen_dead_letter(self, dead_letter_buffer):
        subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_id}`
        # logging.error("listed to %s" % os.environ["IOT_TOPIC_DEAD_LETTER"])
        subscription_path = subscriber.subscription_path(
            "formelife-dev",
            os.environ["IOT_TOPIC_DEAD_LETTER"],
        )

        def callback(message):
            dead_letter_buffer.append(message)
            # logging.error("Received IOT_TOPIC_DEAD_LETTER message: {}".format(message))
            message.ack()

        subscriber.subscribe(subscription_path, callback=callback)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
