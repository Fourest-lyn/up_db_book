from ..db_client import DBClient
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor
from pymongo.collection import Collection
from ...template.user_template import UserTemp
from typing import Optional, List


class UserInterface:
    def __init__(self, conn: DBClient):
        self.conn: connection = conn.conn
        self.cur = self.conn.cursor()
        self.dcur = self.conn.cursor(cursor_factory=DictCursor)
        # self.userCol: Collection = conn.userCol

    def user_id_exist(self, user_id: str) -> bool:
        self.cur.execute(
            "select * from users where user_id = %s",
            (user_id,),
        )
        result = self.cur.fetchone()
        return result is not None
        # cursor = self.userCol.find_one({"user_id": user_id})
        # return cursor is not None

    def add_balance(self, user_id: str, count: int) -> int:
        self.cur.execute(f"select balance from users where user_id = '{user_id}' ")
        balance = self.cur.fetchone()[0]
        if balance + count >= 0:
            self.cur.execute(
                f"update users set balance = balance + {count} where user_id = '{user_id}'"
            )
            self.conn.commit()
            return count
        else:
            return 0

        # result = self.userCol.update_one(
        #     {"user_id": user_id, "balance": {"$gte": min(0, -count)}},
        #     {"$inc": {"balance": count}},
        # )
        # return result.modified_count

    def get_password(self, user_id: str) -> Optional[str]:
        self.cur.execute("select password from users where user_id = %s", (user_id,))
        result = self.cur.fetchone()
        if result is None:
            return None
        else:
            return result[0]

    def get_balance(self, user_id: str) -> Optional[int]:
        self.cur.execute("select balance from users where user_id = %s", (user_id,))
        result = self.cur.fetchone()
        if result is None:
            return None
        else:
            return result[0]

    def get_token(self, user_id: str) -> Optional[str]:
        self.cur.execute("select token from users where user_id = %s", (user_id,))
        result = self.cur.fetchone()
        if result is None:
            return None
        else:
            return result[0]

    def insert_one_user(self, user: UserTemp) -> None:
        # add into users
        user_dict: dict = user.to_dict()
        user_dict.pop("order_id_list", None)
        columns = ", ".join(user_dict.keys())
        values = ", ".join([f"'{value}'" for value in user_dict.values()])
        self.cur.execute(f"insert into users ({columns}) values ({values})")
        # self.userCol.insert_one(user.to_dict())

    def update_token_terminal(self, user_id: str, token: str, terminal: str) -> int:
        if not self.user_id_exist(user_id):
            return 0

        self.cur.execute(
            "update users set token = %s, terminal=%s where user_id = %s",
            (token, terminal, user_id),
        )
        self.conn.commit()
        return 1
        # result = self.userCol.update_one(
        #     {"user_id": user_id}, {"$set": {"token": token, "terminal": terminal}}
        # )
        # return result.modified_count

    def delete_user(self, user_id: str) -> int:
        self.cur.execute("delete from users where user_id = %s", (user_id,))
        delete_count = self.cur.rowcount
        self.conn.commit()
        # result = self.userCol.delete_one({"user_id": user_id})
        return delete_count

    def update_password(
        self, user_id: str, password: str, token: str, terminal: str
    ) -> int:
        if not self.user_id_exist(user_id):
            return 0

        self.cur.execute(
            "update users set password = %s, token = %s, terminal=%s where user_id = %s",
            (password, token, terminal, user_id),
        )
        self.conn.commit()
        return 1

        """
        result = self.userCol.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "password": password,
                    "token": token,
                    "terminal": terminal,
                }
            },
        )
        return result.modified_count
        """

    def add_order(self, user_id: str, order_id: str) -> int:
        # no need because our new table settings.

        # result = self.userCol.update_one(
        #     {"user_id": user_id},
        #     {"$push": {"order_id_list": order_id}},
        # )
        # return result.modified_count
        return 1

    def get_order_list(self, user_id: str) -> Optional[List[str]]:
        self.cur.execute(
            "select order_id from new_order where user_id = %s", (user_id,)
        )
        result = self.cur.fetchall()
        if result is None:
            return None
        else:
            return [order[0] for order in result]

        # result = self.userCol.find_one(
        #     {"user_id": user_id}, {"_id": 0, "order_id_list": 1}
        # )
        # if result is None:
        #     return None
        # else:
        #     return result["order_id_list"]
