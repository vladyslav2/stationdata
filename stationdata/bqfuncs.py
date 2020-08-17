import json
import logging
import traceback

import apache_beam as beam
from apache_beam.io.gcp import bigquery_tools

from . import bqschema # pylint: disable=E


INVALID_DATA = 'invalidData'
FIRMWARE_DATA = 'firmwareData'
WORKOUT_DATA = 'workoutData'
HEART_RATE_DATA = 'heartRateData'

def get_schema(table):
    temp_arr = []
    for val in bqschema.json_schema[table]:
        temp_arr.append('{}:{}'.format(val['name'], val['type']))
    return ','.join(temp_arr)

def decode_pub_sub(pub_sub_msg):
    default_res = {
        "version": "error",
        "data": [],
    }
    try:
        data = json.loads(pub_sub_msg.data.decode('utf-8'))
        if not isinstance(data, dict):
            logging.error("Data should be an object, got: {}", data)
            return default_res
        if "data" not in [*data]:
            track = traceback.format_exc()
            logging.error("Data should have key data but it does not")
            return default_res
        data['device_id'] = pub_sub_msg.attributes['deviceId']
    except Exception as exp: # pylint: disable=W,R
        track = traceback.format_exc()
        logging.error("{}\n{}", exp, track)
        return default_res
    return data


def unpack_messages(json_msg): # pylint: disable=R
    tagged_res = []
    invalid_msgs = []

    if json_msg['_version'] is None:
        logging.error("no _version key found: {}", json_msg)
        json_msg["__error__"] = "wront format, no _version key"
        tagged_res.append(
            beam.pvalue.TaggedOutput(
                INVALID_DATA,
                json_msg,
            )
        )
        return tagged_res
    if json_msg['device_id'] is None:
        logging.error("no device_id key found: {}", json_msg)
        json_msg["__error__"] = "wront format, no device_id key"
        tagged_res.append(
            beam.pvalue.TaggedOutput(
                INVALID_DATA,
                json_msg,
            )
        )
        return tagged_res
    if 'data' not in [*json_msg]:
        logging.error("no data key found: {}", json_msg)
        json_msg["__error__"] = "wront format, no data key"
        tagged_res.append(
            beam.pvalue.TaggedOutput(
                INVALID_DATA,
                json_msg,
            )
        )
        return tagged_res

    for msg in json_msg["data"]:
        if 'type' not in [*msg]:
            msg["__error__"] = "no type key"
            invalid_msgs.append(msg)
            continue
        if is_workout_msg(msg):
            logging.debug("workout_data: {}", msg)
            msg["payload"]['device_id'] = json_msg['device_id']
            tagged_res.append(
                beam.pvalue.TaggedOutput(
                    WORKOUT_DATA,
                    msg,
                )
            )
            continue
        if is_firmware_msg(msg):
            logging.debug("firmwareData: {}", msg)
            try:
                payload = decode_firmware_data(msg["payload"])
                payload['device_id'] = json_msg['device_id']
                payload['timestamp'] = int(int(payload['timestamp'])/1000)
                bqschema.check_data(
                    bqschema.json_schema[FIRMWARE_DATA],
                    payload,
                )
                tagged_res.append(
                    beam.pvalue.TaggedOutput(
                        FIRMWARE_DATA,
                        payload,
                    )
                )
            except Exception as exp: # pylint: disable=W,R
                track = traceback.format_exc()
                logging.error("{}\n{}", exp, track)
                msg["__error__"] = str(exp)
                invalid_msgs.append(msg)
            continue
        if is_heart_rate_msg(msg):
            logging.debug("heartrate: {}", msg)
            try:
                payload = msg["payload"]
                payload['device_id'] = json_msg['device_id']
                payload['timestamp'] = int(int(payload['timestamp'])/1000)
                bqschema.check_data(
                    bqschema.json_schema[HEART_RATE_DATA],
                    payload,
                )
                tagged_res.append(
                    beam.pvalue.TaggedOutput(
                        HEART_RATE_DATA,
                        payload,
                    )
                )
            except Exception as exp: # pylint: disable=W,R
                track = traceback.format_exc()
                logging.error("err {}\n{}\n{}", exp, track, msg)
                msg["__error__"] = str(exp)
                invalid_msgs.append(msg)
            continue
        err_msg = "type not found: %s" % msg['type']
        logging.error(err_msg)
        msg["__error__"] = err_msg
        invalid_msgs.append(msg)

    # It is possible we will have messages with different versions
    # during error we have to know version in order to research problem
    if len(invalid_msgs) != 0:
        tagged_res.append(beam.pvalue.TaggedOutput(
            INVALID_DATA,
            {
                "_version": json_msg["_version"],
                "device_id": json_msg["device_id"],
                "data": invalid_msgs
            }
        ))
    return tagged_res

def is_firmware_msg(elem):
    if 'type' in elem:
        return elem['type'] == "firmwareData"
    if 'event' in elem:
        return elem['event'] == 'rawDataReceived'
    return False

def is_heart_rate_msg(elem):
    if 'type' in elem:
        return elem['type'] == "heartRateData"
    if 'event' in elem:
        return elem['event'] == 'heartRateChanged'
    return False

def is_workout_msg(elem):
    if 'type' in elem:
        return elem['type'] == "workoutData"
    if 'event' in elem:
        return elem['event'] == 'setEnded'\
            or elem['event'] == 'setStarted'\
            or elem['event'] == 'workoutEnded'\
            or elem['event'] == 'repEnded'\
            or elem['event'] == 'workoutStarted'
    return False

# transcode individual pubsub message to the proper format
def decode_firmware_data(elem):
    _type = [*elem["data"]][0]
    _sub_type = ""

    try:
        _sub_type = [*elem["data"][_type]][0]
    except KeyError:
        pass
    except IndexError:
        pass

    elem["type"] = _type
    elem["sub_type"] = _sub_type
    elem["payload"] = json.dumps(elem["data"])
    del elem["data"]
    return elem

def write_to_bigquery(data_type, json_messages, flow_options, iot_topic_dead_letter):
    table = flow_options.bq_firmware
    if data_type == HEART_RATE_DATA:
        table = flow_options.bq_heart_rate

    firmware_results = (
        json_messages[data_type]
        | 'Write {}'.format(data_type) >> beam.io.WriteToBigQuery(
            table=table,
            dataset=flow_options.bq_data_set,
            project=flow_options.project,
            schema=get_schema(data_type),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            insert_retry_strategy=bigquery_tools.RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
            # method=beam.io.WriteToBigQuery.STREAMING_INSERTS,
        )
    )

    ( # pylint: disable=expression-not-assigned
        firmware_results[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
        | 'Bad {} to b'.format(data_type) >>
        beam.Map(lambda el: json.dumps(el).encode('utf-8')).with_output_types(bytes)
        | 'Bad {} to ps'.format(data_type) >> beam.io.WriteToPubSub(topic=iot_topic_dead_letter)
    )
