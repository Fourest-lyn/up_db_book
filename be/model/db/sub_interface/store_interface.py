from ..db_client import DBClient, db_column_list
from pymongo.collection import Collection
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor
from ...template.store_template import StoreBookTmp, StoreTemp
from ... import error
from typing import List, Dict, Any, Optional
from ...template.book_info import BookInfoTemp


class StoreInterface:
    def __init__(self, conn: DBClient):
        self.conn: connection = conn.conn
        self.cur = self.conn.cursor()
        self.dcur = self.conn.cursor(cursor_factory=DictCursor)
        # self.storeCol: Collection = conn.storeCol
        # self.bookInfoCol: Collection = conn.bookInfoCol

    def __del__(self):
        self.cur.close()
        self.dcur.close()

    def book_id_exist(self, store_id, book_id) -> bool:
        # cursor = self.storeCol.find_one({"store_id": store_id, "book_list.book_id": book_id})
        self.cur.execute(
            "select * from book_list where store_id = %s and book_id = %s",
            (store_id, book_id),
        )
        result = self.cur.fetchone()
        return result is not None

    def store_id_exist(self, store_id) -> bool:
        # cursor = self.storeCol.find_one({"store_id": store_id})
        # 执行SQL查询，表格为stores
        self.cur.execute(
            "SELECT EXISTS(SELECT 1 FROM stores WHERE store_id = %s)", (store_id,)
        )
        # 获取查询结果
        result = self.cur.fetchone()[0]
        return result

    def find_book(self, store_id: str, book_id: str) -> Optional[StoreBookTmp]:
        self.dcur.execute(
            "select * from book_list where store_id = %s and book_id = %s",
            (store_id, book_id),
        )
        """
        pipeline: List[Dict[str, Any]] = [
            {"$match": {"store_id": store_id}},
            {"$unwind": "$book_list"},
            {"$match": {"book_list.book_id": book_id}},
            {"$replaceRoot": {"newRoot": "$book_list"}},
        ]
        results = self.storeCol.aggregate(pipeline=pipeline)
        doc = next(results, None)
        """
        result = self.dcur.fetchone()
        if result is None:
            return None
        else:
            return StoreBookTmp.from_dict(result)

    """
    def get_book_info(self, book_info_id: str) -> Optional[Dict[str, Any]]:
        # may to do: we need to find book_info and the book_tags

        # get book_info
        self.dcur.execute("select * from book_info where book_info_id=%s",(book_info_id,))
        # doc = self.bookInfoCol.find_one({"_id": book_info_id})


        # get book_tags

        doc = 0
        if doc is None:
            return None
        else:
            return doc["book_info"]
    """

    def add_stock_level(self, store_id: str, book_id: str, count: int) -> int:
        self.cur.execute(
            f"select stock_level from book_list where store_id = '{store_id}' and book_id = '{book_id}'"
        )
        stock_level = self.cur.fetchone()[0]
        if stock_level + count >= 0:
            self.cur.execute(
                f"update book_list set stock_level = stock_level + {count} where store_id = '{store_id}' and book_id = '{book_id}'"
            )
            self.conn.commit()
            return count
        else:
            return 0
        """
        result = self.storeCol.update_one(
            {"store_id": store_id},
            {"$inc": {"book_list.$[elem].stock_level": count}},
            array_filters=[
                {"elem.book_id": book_id, "elem.stock_level": {"$gte": min(0, -count)}}
            ],
        )
        return result.modified_count
        """

    def get_store_seller_id(self, store_id: str) -> Optional[str]:
        self.cur.execute("SELECT userid FROM stores WHERE store_id = %s", (store_id,))
        result = self.cur.fetchone()
        # result = self.storeCol.find_one({"store_id": store_id}, {"_id": 0, "user_id": 1})
        if result is None:
            return None
        else:
            return result[0]

    def insert_one_book(
        self, store_id: str, book_id: str, stock_level: int, book_info: str
    ) -> None:
        new_book_info = BookInfoTemp(book_info, store_id)
        # add into book_info
        book_dict: dict = new_book_info.to_dict()
        book_dict.pop("tags", None)
        columns = ", ".join(book_dict.keys())
        values = ", ".join([f"'{value}'" for value in book_dict.values()])
        self.cur.execute(f"insert into book_info ({columns}) values ({values})")

        # get book_info_id
        book_info_id = self.cur.fetchone()[0]

        # add into book_list, maybe not need
        index: str = ",".join(db_column_list["book_list"])
        self.cur.execute(
            f"insert into book_list ({index}) values ('{store_id}','{book_id}','{book_info_id}','{stock_level}')"
        )

        # add into book_tags
        data = [(book_info_id, tg) for tg in new_book_info.to_dict()["tags"]]
        sql = "insert into book_tags (book_id, tag) values (%s, %s)"
        self.cur.executemany(sql, data)

        self.conn.commit()

        # result = self.bookInfoCol.insert_one(new_book_info.to_dict())
        # info_id = result.inserted_id
        # new_book = StoreBookTmp(
        #     book_id=book_id, book_info_id=info_id, stock_level=stock_level
        # )
        # self.storeCol.update_one(
        #     {"store_id": store_id}, {"$push": {"book_list": new_book.to_dict()}}
        # )

    def insert_one_store(self, new_store: StoreTemp) -> None:
        # add into store
        store_id = new_store.store_id
        index: str = ",".join(db_column_list["stores"])
        self.cur.execute(
            f"insert into stores ({index}) values ('{store_id}', '{new_store.user_id}')"
        )

        # add into book_list
        index = ",".join(db_column_list["book_list"])
        for bk in new_store.book_list:
            self.cur.execute(
                f"insert into book_list ({index}) values ('{store_id}','{bk.book_id}','{bk.book_info_id}','{bk.stock_level}')"
            )
        self.conn.commit()

        # self.storeCol.insert_one(new_store.to_dict())
