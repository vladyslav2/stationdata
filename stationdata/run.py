"""A streaming word-counting workflow.
"""
from __future__ import absolute_import

import json
import os
import time
import logging

import apache_beam as beam
from apache_beam.options import pipeline_options
# from apache_beam.io.gcp import bigquery_tools

from . import bqfuncs # pylint: disable=E
# import apache_beam.transforms.window as window


def required_parameters(parser):
    parser.add_argument(
        '--runner',
        required=True,
        help=('Run dataflow locally (direct) or in the cloud (dataflow)'))
    parser.add_argument(
        '--iot_topic_telemetry',
        required=True,
        help=('Input PubSub topic of the form <TOPIC>'))
    parser.add_argument(
        '--iot_topic_dead_letter',
        required=True,
        help=('Dead Letter PubSub topic of the form <TOPIC>'))
    parser.add_argument(
        '--cloud_topic_workout',
        required=True,
        help=('Output PubSub topic of the form <TOPIC>'))
    parser.add_argument(
        '--bq_data_set',
        required=True,
        help=('Input bigQuery dataset name <dataSet>'))
    parser.add_argument(
        '--bq_firmware',
        required=True,
        help=('Output firmware bigQuery table <firmwareData>'))
    parser.add_argument(
        '--bq_heart_rate',
        required=True,
        help=('Output heart rate bigQuery table <heartRate>'))

class StationDataOptions(pipeline_options.GoogleCloudOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        required_parameters(parser)

def run(argv=None):
    """Build and run the pipeline."""

    logging.error('run is executed')
    level = os.environ.get("IOT_LOG_LEVEL", "DEBUG")
    error_level = logging._nameToLevel.get(level.upper()) # pylint: disable=W
    if error_level is not None:
        logging.getLogger().setLevel(error_level)
    else:
        logging.getLogger().setLevel(logging.INFO)
    dataflow_options = StationDataOptions(argv)

    if dataflow_options.runner == "Dataflow":
        dataflow_options.enable_streaming_engine = True # pylint: disable=W

    start_dataflow(dataflow_options)


def start_dataflow(flow_options):

    logging.error('start_dataflow is executed')
    iot_topic_telemetry = 'projects/{}/topics/{}'.format(flow_options.project, flow_options.iot_topic_telemetry)
    iot_topic_dead_letter = 'projects/{}/topics/{}'.format(flow_options.project, flow_options.iot_topic_dead_letter)
    cloud_topic_workout = 'projects/{}/topics/{}'.format(flow_options.project, flow_options.cloud_topic_workout)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    flow_options.view_as(
        pipeline_options.SetupOptions).save_main_session = True
    flow_options.view_as(pipeline_options.StandardOptions).streaming = True
    pipe = beam.Pipeline(options=flow_options)

    json_messages = (
        pipe
        | beam.io.ReadFromPubSub(topic=iot_topic_telemetry, with_attributes=True)
        | 'Json encode' >> beam.Map(bqfuncs.decode_pub_sub)
        | 'Unpack msgs' >> beam.transforms.ParDo(bqfuncs.unpack_messages).with_outputs(
            bqfuncs.INVALID_DATA, bqfuncs.FIRMWARE_DATA, bqfuncs.WORKOUT_DATA, bqfuncs.HEART_RATE_DATA, main='main')
    )

    """
    ( # pylint: disable=expression-not-assigned
        json_messages[bqfuncs.INVALID_DATA]
        | 'Invalid msgs to b' >> beam.Map(lambda el: json.dumps(el).encode('utf-8')).with_output_types(bytes)
        | 'Unvalid msgs to dl' >> beam.io.WriteToPubSub(topic=iot_topic_dead_letter)
    )
    """

    # bqfuncs.write_to_bigquery(bqfuncs.FIRMWARE_DATA, json_messages, flow_options, iot_topic_dead_letter)
    bqfuncs.write_to_bigquery(bqfuncs.HEART_RATE_DATA, json_messages, flow_options, iot_topic_dead_letter)

    """
    ( # pylint: disable=expression-not-assigned
        json_messages[bqfuncs.WORKOUT_DATA]
        | 'workout data to bytes' >> beam.Map(lambda el: json.dumps(el).encode('utf-8')).with_output_types(bytes)
        | 'Write workout data' >> beam.io.WriteToPubSub(topic=cloud_topic_workout)
    )
    """

    result = pipe.run()
    result.wait_until_finish()
    """
    if flow_options.runner != "Dataflow":
        result.wait_until_finish()
    else:
        # Give dataflow 30 seconds to report any issues
        time.sleep(30)
    """
