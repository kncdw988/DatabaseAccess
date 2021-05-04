import json
from datetime import datetime
from functools import wraps

from bson import ObjectId


def singleton(cls):
    instances = {}
    @wraps(cls)
    def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return getinstance

@singleton
class Utils:
    default_error_code = '0'
    default_success_code = '1'
    datetime_format_full = '%Y-%m-%d %H:%M:%S'
    datetime_format_time = '%H:%M:%S'
    datetime_format_date = '%Y-%m-%d'
    datetime_without_year = '%m/%d %H:%M'
    datetime_without_second = '%Y-%m-%d %H:%M'
    datetime_format_full_file_name = '%Y-%m-%d %H-%M-%S'


class Encoder(json.JSONEncoder):
    """
    用在JSON序列化中, 处理时间及ObjectId类型数据
    """

    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.strftime(Utils.datetime_format_full)
        elif str(obj).lower() == 'nan':
            return None
        else:
            return obj.__str__()

    