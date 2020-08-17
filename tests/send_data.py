import os
import json

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(
    os.environ['PROJECT_ID'],
    os.environ['IOT_TOPIC_TELEMETRY'],
)

workout_file = json.load(open('tests/workouts_log.json'))
full_sample = json.load(open('tests/full_sample.json'))
broken_msgs = json.load(open('tests/broken_msgs.json'))


"""
ToDo
Add dead letter checks

"""

def publish(data):
    attrs = {
        "deviceId": "stationTestV3",
        "projectId": "formelife-dev",
        "subFolder": ""
    }

    # Data must be a bytestring
    data = json.dumps(data).encode('utf-8')
    # When you publish a message, the client returns a Future.
    publisher.publish(topic_path, data=data, **attrs)


if __name__ == "__main__":
    publish(broken_msgs)
    publish(full_sample)
