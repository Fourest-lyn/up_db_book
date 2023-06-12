import pytest

from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
from fe.access.new_seller import register_new_seller
from fe.access.book import Book
import uuid


class TestOrderDeliver:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_order_deliver_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_order_deliver_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_order_deliver_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id
        self.gen_book = GenBook(self.seller_id, self.store_id)
        ok, buy_book_id_list = self.gen_book.gen(
            non_exist_book_id=False, low_stock_level=False, max_book_count=5
        )
        self.buy_book_info_list = self.gen_book.buy_book_info_list
        assert ok
        b = register_new_buyer(self.buyer_id, self.password)
        self.buyer = b
        code, self.order_id = b.new_order(self.store_id, buy_book_id_list)
        assert code == 200
        self.total_price = 0
        for item in self.buy_book_info_list:
            book: Book = item[0]
            num = item[1]
            if book.price is None:
                continue
            else:
                self.total_price = self.total_price + book.price * num
        yield

    def test_deliver_order(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.buyer.payment(self.order_id)
        assert code == 200
        code = self.gen_book.seller.ship_order(self.order_id)
        assert code == 200
        code = self.buyer.receive_order(self.order_id)
        assert code == 200

    def test_deliver_error_status(self):
        code = self.buyer.add_funds(self.total_price)
        assert code == 200
        code = self.gen_book.seller.ship_order(self.order_id)
        assert code == 520
        code = self.buyer.receive_order(self.order_id)
        assert code == 520
        code = self.buyer.payment(self.order_id)
        assert code == 200
        code = self.buyer.receive_order(self.order_id)
        assert code == 520
        code = self.gen_book.seller.ship_order(self.order_id)
        assert code == 200
        code = self.gen_book.seller.ship_order(self.order_id)
        assert code == 520
        code = self.buyer.receive_order(self.order_id)
        assert code == 200
        code = self.buyer.receive_order(self.order_id)
        assert code == 520
