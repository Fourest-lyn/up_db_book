from ..db_client import DBClient
from pymongo.collection import Collection
from ...template.user_template import UserTemp
from typing import Optional, List


class UserInterface:
    def __init__(self, conn: DBClient):
        self.userCol: Collection = conn.userCol

    def user_id_exist(self, user_id: str) -> bool:
        cursor = self.userCol.find_one({"user_id": user_id})
        return cursor is not None

    # def find_user(self, user_id:str) -> Optional[UserTemp]:
    #     result = self.userCol.find_one({"user_id": user_id})
    #     if result is None:
    #         return None
    #     else:
    #         return UserTemp.from_dict(result)

    def add_balance(self, user_id: str, count: int) -> int:
        result = self.userCol.update_one(
            {"user_id": user_id, "balance": {"$gte": min(0, -count)}},
            {"$inc": {"balance": count}},
        )
        return result.modified_count

    def get_password(self, user_id: str) -> Optional[str]:
        result = self.userCol.find_one({"user_id": user_id}, {"_id": 0, "password": 1})
        if result is None:
            return None
        else:
            return result["password"]

    def get_balance(self, user_id: str) -> Optional[int]:
        result = self.userCol.find_one({"user_id": user_id}, {"_id": 0, "balance": 1})
        if result is None:
            return None
        else:
            return result["balance"]

    def get_token(self, user_id: str) -> Optional[str]:
        result = self.userCol.find_one({"user_id": user_id}, {"_id": 0, "token": 1})
        if result is None:
            return None
        else:
            return result["token"]

    def insert_one_user(self, user: UserTemp) -> None:
        self.userCol.insert_one(user.to_dict())

    def update_token_terminal(self, user_id: str, token: str, terminal: str) -> int:
        result = self.userCol.update_one(
            {"user_id": user_id}, {"$set": {"token": token, "terminal": terminal}}
        )
        return result.modified_count

    def delete_user(self, user_id: str) -> int:
        result = self.userCol.delete_one({"user_id": user_id})
        return result.deleted_count

    def update_password(
        self, user_id: str, password: str, token: str, terminal: str
    ) -> int:
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

    def add_order(self, user_id: str, order_id: str) -> int:
        result = self.userCol.update_one(
            {"user_id": user_id},
            {"$push": {"order_id_list": order_id}},
        )
        return result.modified_count

    def get_order_list(self, user_id: str) -> Optional[List[str]]:
        result = self.userCol.find_one(
            {"user_id": user_id}, {"_id": 0, "order_id_list": 1}
        )
        if result is None:
            return None
        else:
            return result["order_id_list"]
