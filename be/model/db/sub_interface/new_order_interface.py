from ..db_client import DBClient, db_column_list

# from pymongo.collection import Collection
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
        self.dcur = self.conn.cursor(cursor_factory=DictCursor)
        # self.newOrderCol: Collection = conn.newOrderCol

    def insert_new_order(self, new_order: NewOrderTemp):
        # insert into new_order
        index: str = ",".join(db_column_list["new_order"])
        order_dict: dict = new_order.to_dict()
        order_dict.pop("book_list", None)
        values = ", ".join([f"'{value}'" for value in order_dict.values()])
        self.cur.execute(f"insert into new_order ({index}) values ({values})")

        # insert into order_book
        index: str = ",".join(db_column_list["order_book"])
        order_id = new_order.order_id
        for bk in new_order.book_list:
            self.cur.execute(
                f"insert into new_order ({index}) values ('{order_id}','{bk.book_id}','{bk.count}','{bk.price}')"
            )

        self.conn.commit()

        # self.newOrderCol.insert_one(new_order.to_dict())

    def find_new_order(self, order_id: str) -> Optional[NewOrderTemp]:
        # get order information
        self.dcur.execute(
            "select * from new_order where order_id = %s ",
            (order_id,),
        )
        order_dict = self.dcur.fetchone()

        if not order_dict:
            return None

        # get order book_list
        self.dcur.execute(
            "select * from order_book where order_id = %s ",
            (order_id,),
        )
        get_book_list = self.dcur.fetchall()

        book_list = [
            NewOrderBookItemTemp.from_dict(book_item) for book_item in get_book_list
        ]

        # mix them

        new_order = NewOrderTemp(
            order_id=order_dict["order_id"],
            user_id=order_dict["user_id"],
            store_id=order_dict["store_id"],
            book_list=book_list,
            create_time=order_dict["create_time"],
            status=STATUS(order_dict["status"]),
        )

        return new_order

        """
        doc = self.newOrderCol.find_one({"order_id": order_id})
        if doc is None:
            return None
        else:
            return NewOrderTemp.from_dict(doc)
        """

    """
    def delete_order(self, order_id: str) -> int:
        result = self.newOrderCol.delete_one({"order_id": order_id})
        self.cur.execute("delete from new_order where order_id = %s", (order_id,))
        self.cur.execute("delete from order_book where order_id = %s", (order_id,))
        return result.deleted_count
    """

    def order_id_exist(self, order_id: str) -> bool:
        self.cur.execute(
            "select exists(select 1 from new_order where order_id=%s",
            (order_id,),
        )
        result = self.cur.fetchone()[0]
        return result

        # cursor = self.newOrderCol.find_one({"order_id": order_id})
        # return cursor is not None

    def update_new_order_status(self, order_id: str, status: STATUS) -> int:
        self.cur.execute(
            f"update new_order set status= {status} where order_id = '{order_id}'"
        )
        self.conn.commit()
        # result = self.newOrderCol.update_one(
        #     {"order_id": order_id}, {"$set": {"status": status.value}}
        # )
        return status

    def find_order_status(self, order_id: str) -> Optional[STATUS]:
        self.cur.execute(f"select status from new_order where order_id = '{order_id}'")
        order_status = self.cur.fetchone()[0]
        if order_status is None:
            return None
        else:
            return STATUS(order_status)

        # result = self.newOrderCol.find_one(
        #     {"order_id": order_id}, {"_id": 0, "status": 1}
        # )
        # if result is None:
        #     return None
        # else:
        #     return STATUS(result["status"])

    def auto_cancel_expired_order(
        self, current_time: int, expire_time: int
    ) -> List[str]:
        # for all order that satisfy order.create_time + expire_time >= current_time
        # and status == INIT(0), change its status to CANCELLED(4)
        # returns: a list of order_id, represent all cancelled order

        # the need to be canceled status:
        c_status = STATUS.INIT
        delta_time = current_time - expire_time
        self.cur.execute(
            f"select order_id from new_order where status='{c_status}' and create_time >= '{delta_time}' "
        )
        orders = self.cur.fetchall()
        order_id_list = [order[0] for order in orders]

        # expired status:
        e_status = STATUS.CANCELED
        self.cur.execute(
            f"UPDATE new_order SET status='{e_status}' WHERE status='{c_status}' AND create_time >= '{delta_time}'"
        )

        self.conn.commit()

        return order_id_list

        """
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
        """
