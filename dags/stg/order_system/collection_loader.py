from datetime import datetime, timedelta
from typing import Dict, List

from repositories.mongo_connect import MongoConnect


class CollectionLoader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_documents(self, collection_name: str, limit) -> List[Dict]:
        default_load_period = datetime.utcnow() - timedelta(days=8)
        filter = {'update_ts': {'$gt': default_load_period}}
        sort = [('update_ts', 1)]
        docs = list(self.dbs.get_collection(collection_name).find(filter=filter, sort=sort, limit=limit))
        return docs
