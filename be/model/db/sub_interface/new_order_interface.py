from ..db_client import DBClient
from pymongo.collection import Collection
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor
from ...template.new_order_template import NewOrderTemp, NewOrderBookItemTemp
from typing import Optional, List, Dict, Any
from ...template.new_order_template import STATUS


class NewOrderInterface:
    def __init__(self, conn: DBClient):
        self.conn = conn.conn
        self.cur = self.conn.cursor()
        self.dcur = self.conn.cursor()
        self.newOrderCol: Collection = conn.newOrderCol

    def insert_new_order(self, new_order: NewOrderTemp):
        self.newOrderCol.insert_one(new_order.to_dict())

    def find_new_order(self, order_id: str) -> Optional[NewOrderTemp]:
        doc = self.newOrderCol.find_one({"order_id": order_id})
        if doc is None:
            return None
        else:
            return NewOrderTemp.from_dict(doc)

    def delete_order(self, order_id: str) -> int:
        result = self.newOrderCol.delete_one({"order_id": order_id})
        return result.deleted_count

    def order_id_exist(self, order_id: str) -> bool:
        cursor = self.newOrderCol.find_one({"order_id": order_id})
        return cursor is not None

    def update_new_order_status(self, order_id: str, status: STATUS) -> int:
        result = self.newOrderCol.update_one(
            {"order_id": order_id}, {"$set": {"status": status.value}}
        )
        return result.modified_count

    def find_order_status(self, order_id: str) -> Optional[STATUS]:
        result = self.newOrderCol.find_one(
            {"order_id": order_id}, {"_id": 0, "status": 1}
        )
        if result is None:
            return None
        else:
            return STATUS(result["status"])

    def auto_cancel_expired_order(
        self, current_time: int, expire_time: int
    ) -> List[str]:
        # for all order that satisfy order.create_time + expire_time >= current_time
        # and status == INIT(0), change its status to CANCELLED(4)
        # returns: a list of order_id, represent all cancelled order

        pipeline: List[Dict[str, Any]] = [
            {"$match": {"status": 0}},
            {
                "$project": {
                    "order_id": 1,
                    "create_time": 1,
                    "status": 1,
                    "expired": {
                        "$gte": [
                            {"$add": ["$create_time", expire_time]},
                            current_time,
                        ]
                    },
                }
            },
            {"$match": {"expired": True}},
        ]
        results = self.newOrderCol.aggregate(pipeline=pipeline)
        order_id_list = []
        for doc in results:
            order_id_list.append(doc["order_id"])
            self.update_new_order_status(doc["order_id"], STATUS.CANCELED)
        return order_id_list
