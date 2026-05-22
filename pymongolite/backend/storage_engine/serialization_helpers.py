import json
from datetime import datetime

class MongoLiteJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"$date":obj.isoformat()}

        return super().default(obj)


class MongoLiteJSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, object_hook=self.object_hook,
                         **kwargs)

    def object_hook(self, d): 
        if '$date' in d:
            return datetime.fromisoformat(d['$date'])
            
        return d
