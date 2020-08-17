_MAXINT64 = (1 << 63) - 1
_MININT64 = -(1 << 63)

def find_by_name(schema, name):
    for k in schema:
        if k["name"] == name:
            return k
    return {}

def check_data(schema, payload):
    for key, val in payload.items():
        desc = find_by_name(schema, key)
        if 'type' in desc:
            if desc["mode"] != "required" and val == "":
                continue
            if desc["type"] == "timestamp" or \
                desc["type"] == "integer":
                val = int(val)
                if _MININT64 <= val <= _MAXINT64 == False:
                    raise TypeError('{}: can not encode {} as a 64-bit integer'.format(key, val))
                continue
            if desc["type"] == "string":
                val = str(val)
                continue
            raise SystemError("{}: not check for {}".format(key, desc["type"]))
        raise SystemError("Field not found {}:{}".format(key, val))

json_schema = {
    'firmwareData': [
        {
            "name": "timestamp",
            "type": "timestamp",
            "mode": "required"
        },
        {
            "name": "device_id",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "user_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "plan_workout_id",
            "type": "integer",
            "mode": "nullable"
        },
        {
            "name": "plan_set_id",
            "type": "integer",
            "mode": "nullable"
        },
        {
            "name": "history_workout_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "history_set_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "history_rep_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "message_id",
            "type": "integer",
            "mode": "nullable"
        },
        {
            "name": "payload",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "type",  # response, error, notification, log
            "type": "string",
            "mode": "required"
        },
        {
            "name": "sub_type",  # type of the response, type of the error and etc
            "type": "string",
            "mode": "required"
        },
    ],
    'heartRateData': [
        {
            "name": "timestamp",
            "type": "timestamp",
            "mode": "required"
        },
        {
            "name": "device_id",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "user_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "plan_workout_id",
            "type": "integer",
            "mode": "nullable"
        },
        {
            "name": "plan_set_id",
            "type": "integer",
            "mode": "nullable"
        },
        {
            "name": "history_workout_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "history_set_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "history_rep_id",
            "type": "string",
            "mode": "nullable"
        },
        {
            "name": "monitor_device_id",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "monitor_device_name",
            "type": "string",
            "mode": "required"
        },
        {
            "name": "bpm",
            "type": "integer",
            "mode": "required"
        },
    ]
}
