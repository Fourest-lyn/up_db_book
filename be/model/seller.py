from pymongo.errors import PyMongoError
from . import error
from .db.interface import DBInterface
from .template.store_template import StoreBookTmp, StoreTemp
from typing import List, Tuple, Optional
from .template.new_order_template import STATUS


class Seller:
    def __init__(self):
        self.db: DBInterface = DBInterface()

    def add_book(
        self,
        user_id: str,
        store_id: str,
        book_id: str,
        book_json_str: str,
        stock_level: int,
    ):
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.db.store.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if self.db.store.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)

            self.db.store.insert_one_book(
                store_id=store_id,
                book_id=book_id,
                book_info=book_json_str,
                stock_level=stock_level,
            )
        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def add_stock_level(
        self, user_id: str, store_id: str, book_id: str, add_stock_level: int
    ):
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.db.store.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if not self.db.store.book_id_exist(store_id, book_id):
                return error.error_non_exist_book_id(book_id)

            self.db.store.add_stock_level(
                store_id=store_id, book_id=book_id, count=add_stock_level
            )
        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def create_store(self, user_id: str, store_id: str) -> Tuple[int, str]:
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if self.db.store.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)

            new_store = StoreTemp(store_id=store_id, user_id=user_id)
            self.db.store.insert_one_store(new_store)
        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def get_book_stock_level(
        self, user_id: str, store_id: str, book_id: str
    ) -> Tuple[int, str, int]:
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.db.store.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if not self.db.store.book_id_exist(store_id, book_id):
                return error.error_non_exist_book_id(book_id)

            book = self.db.store.find_book(store_id, book_id)
            if book is None:
                return error.error_non_exist_book_id(book_id)
            stock_level = book.stock_level
        except PyMongoError as e:
            return 528, "{}".format(str(e)), -1
        except BaseException as e:
            return 530, "{}".format(str(e)), -1
        return 200, "ok", stock_level

    def ship_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.db.new_order.order_id_exist(order_id):
                return error.error_invalid_order_id(order_id)

            status: Optional[STATUS] = self.db.new_order.find_order_status(order_id)
            if status is None:
                return error.error_invalid_order_id(order_id)
            if status != STATUS.PAID:
                return error.error_order_status(order_id, status, STATUS.PAID)

            self.db.new_order.update_new_order_status(order_id, STATUS.SHIPPED)
        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def auto_cancel_expired_order(
        self, current_time: int, expire_time: int
    ) -> Tuple[int, str, List[str]]:
        try:
            canceled_order_id_list = self.db.new_order.auto_cancel_expired_order(
                current_time, expire_time
            )
            for order_id in canceled_order_id_list:
                order = self.db.new_order.find_new_order(order_id)
                if order is None:
                    continue
                for book_item in order.book_list:
                    self.db.store.add_stock_level(
                        order.store_id, book_item.book_id, book_item.count
                    )
        except PyMongoError as e:
            return 528, "{}".format(str(e)), []
        except BaseException as e:
            return 530, "{}".format(str(e)), []

        return 200, "ok", canceled_order_id_list
