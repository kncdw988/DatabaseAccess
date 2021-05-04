from datetime import datetime
import json
from mod_common.utils import Utils, Encoder

class RedisDataSerializer:

    def __init__(self):
        pass

    @classmethod
    def dumps(cls, obj:dict):
        if isinstance(obj, dict):
            rt = {}
            for key, value in obj.items():
                if isinstance(value, (bytes, str, float)):
                    rt.update({key: value})
                elif isinstance(value, bool):
                    rt.update({key: json.dumps(value)})
                elif isinstance(value, int):
                    rt.update({key: value})
                elif isinstance(value, dict):
                    rt.update({key: json.dumps(value, cls=Encoder)})
                elif isinstance(value, datetime):
                    rt.update({key: value.strftime(Utils.datetime_format_full)})
                else:
                    rt.update({key: str(value)})
            return rt
        else:
            return {}

    @classmethod
    def loads(cls, obj:dict):
        if isinstance(obj, dict):
            rt = {}
            for key, value in obj.items():
                if isinstance(value, str):
                    try:
                        rt.update({key: json.loads(value)})
                    except:
                        rt.update({key: value})
                else:
                    rt.update({key, value})
            return rt
        else:
            return {}