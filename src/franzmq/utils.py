import json
import datetime
from enum import Enum   

class StrEnum(str, Enum):
    def __str__(self):
        return self.value


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ServiceType):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, IndexType):
            return str(obj)
        if isinstance(obj, DataType):
            return str(obj)
        
        return super().default(obj)