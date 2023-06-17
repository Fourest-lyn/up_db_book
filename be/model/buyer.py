import uuid
import json
import logging
from pymongo.errors import PyMongoError
from .db.interface import DBInterface
from . import error
from typing import List, Tuple, Dict, Any, Optional
from .template.new_order_template import NewOrderTemp, NewOrderBookItemTemp
from .template.store_template import StoreBookTmp
from .template.user_template import UserTemp
from .template.new_order_template import STATUS


class Buyer:
    def __init__(self):
        self.db: DBInterface = DBInterface()

    def new_order(
        self,
        user_id: str,
        store_id: str,
        id_and_count: List[Tuple[str, int]],
        create_time: int,
    ) -> Tuple[int, str, str]:
        order_id = ""
        try:
            if not self.db.user.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.db.store.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))
            order_id = uid

            book_list = []
            for book_id, count in id_and_count:
                match_book = self.db.store.find_book(store_id, book_id)
                if match_book is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)
                stock_level = match_book.stock_level
                book_info_id = match_book.book_info_id
                price = self.db.searcher.get_one_info_by_info_id(book_info_id, "price")
                assert price is not None

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                modified_count = self.db.store.add_stock_level(
                    store_id, book_id, -count
                )
                if modified_count == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)
                book_list.append(
                    NewOrderBookItemTemp(book_id=book_id, count=count, price=price)
                )

            new_order = NewOrderTemp(
                order_id=uid,
                user_id=user_id,
                store_id=store_id,
                book_list=book_list,
                create_time=create_time,
            )
            self.db.new_order.insert_new_order(new_order)
            self.db.user.add_order(user_id, order_id)

        except PyMongoError as e:
            logging.info("528, {}".format(str(e)))
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            logging.info("530, {}".format(str(e)))
            print("530, {}".format(str(e)))
            return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

    def payment(self, user_id: str, password: str, order_id: str) -> Tuple[int, str]:
        try:
            match_order = self.db.new_order.find_new_order(order_id)
            if match_order is None:
                return error.error_invalid_order_id(order_id)

            order_id = match_order.order_id
            buyer_id = match_order.user_id
            store_id = match_order.store_id
            book_list = match_order.book_list

            if buyer_id != user_id:
                return error.error_authorization_fail()

            balance = self.db.user.get_balance(user_id)
            match_password = self.db.user.get_password(user_id)
            if balance is None or match_password is None:
                return error.error_non_exist_user_id(buyer_id)
            if password != match_password:
                return error.error_authorization_fail()

            seller_id = self.db.store.get_store_seller_id(store_id)
            if seller_id is None:
                return error.error_non_exist_store_id(store_id)

            if not self.db.user.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            total_price = 0
            for row in book_list:
                count = row.count
                price = row.price
                total_price = total_price + price * count

            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            modified_count = self.db.user.add_balance(buyer_id, -total_price)
            if modified_count == 0:
                return error.error_not_sufficient_funds(order_id)

            modified_count = self.db.user.add_balance(seller_id, total_price)

            if modified_count == 0:
                return error.error_non_exist_user_id(seller_id)

            status: Optional[STATUS] = self.db.new_order.find_order_status(order_id)
            if status is None:
                return error.error_invalid_order_id(order_id)
            if status != STATUS.INIT:
                return error.error_order_status(order_id, status, STATUS.INIT)

            modified_count = self.db.new_order.update_new_order_status(
                order_id, STATUS.PAID
            )
            if modified_count == 0:
                return error.error_invalid_order_id(order_id)

        except PyMongoError as e:
            return 528, "{}".format(str(e))

        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> Tuple[int, str]:
        try:
            result = self.db.user.get_password(user_id)
            if result is None:
                return error.error_authorization_fail()

            if result != password:
                return error.error_authorization_fail()

            modified_count = self.db.user.add_balance(user_id, add_value)
            if modified_count == 0:
                return error.error_non_exist_user_id(user_id)

        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive_order(self, user_id: str, order_id: str) -> Tuple[int, str]:
        try:
            match_order = self.db.new_order.find_new_order(order_id)
            if match_order is None:
                return error.error_invalid_order_id(order_id)

            order_id = match_order.order_id
            buyer_id = match_order.user_id

            if buyer_id != user_id:
                return error.error_authorization_fail()

            status: Optional[STATUS] = self.db.new_order.find_order_status(order_id)
            if status is None:
                return error.error_invalid_order_id(order_id)
            if status != STATUS.SHIPPED:
                return error.error_order_status(order_id, status, STATUS.SHIPPED)

            modified_count = self.db.new_order.update_new_order_status(
                order_id, STATUS.RECEIVED
            )
            if modified_count == 0:
                return error.error_invalid_order_id(order_id)

        except PyMongoError as e:
            return 528, "{}".format(str(e))

        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def query_order(
        self, user_id: str, order_id: str
    ) -> Tuple[int, str, Dict[str, Any]]:
        try:
            match_order = self.db.new_order.find_new_order(order_id)
            if match_order is None:
                return error.error_invalid_order_id(order_id)
            if match_order.user_id != user_id:
                return error.error_authorization_fail()
        except PyMongoError as e:
            return 528, "{}".format(str(e)), {}
        except BaseException as e:
            return 530, "{}".format(str(e)), {}
        return 200, "ok", match_order.to_dict()

    def query_order_id_list(
        self, user_id: str, password: str
    ) -> Tuple[int, str, List[str]]:
        try:
            result = self.db.user.get_password(user_id)
            if result is None:
                return error.error_non_exist_user_id(user_id) + ([],)
            if result != password:
                return error.error_authorization_fail() + ([],)

            order_list = self.db.user.get_order_list(user_id)
            if order_list is None:
                return error.error_non_exist_user_id(user_id) + ([],)
        except PyMongoError as e:
            return 528, "{}".format(str(e)), []
        except BaseException as e:
            return 530, "{}".format(str(e)), []
        return 200, "ok", order_list

    def cancel_order(
        self, user_id: str, password: str, order_id: str
    ) -> Tuple[int, str]:
        try:
            result = self.db.user.get_password(user_id)
            if result is None:
                return error.error_non_exist_user_id(user_id)
            if result != password:
                return error.error_authorization_fail()

            match_order = self.db.new_order.find_new_order(order_id)
            if match_order is None:
                return error.error_invalid_order_id(order_id)
            if match_order.user_id != user_id:
                return error.error_authorization_fail()

            status: Optional[STATUS] = self.db.new_order.find_order_status(order_id)
            if status is None:
                return error.error_invalid_order_id(order_id)
            if status != STATUS.INIT:
                return error.error_order_status(order_id, status, STATUS.INIT)

            modified_count = self.db.new_order.update_new_order_status(
                order_id, STATUS.CANCELED
            )
            if modified_count == 0:
                return error.error_invalid_order_id(order_id)

            for book_item in match_order.book_list:
                self.db.store.add_stock_level(
                    match_order.store_id, book_item.book_id, book_item.count
                )

        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"
