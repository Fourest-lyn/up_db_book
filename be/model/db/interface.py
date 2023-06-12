from .db_client import DBClient, get_db_conn
from .sub_interface.new_order_interface import NewOrderInterface
from .sub_interface.store_interface import StoreInterface
from .sub_interface.user_interface import UserInterface
from .sub_interface.searcher_interface import SearcherInterface


class DBInterface:
    def __init__(self):
        self.conn: DBClient = get_db_conn()
        self.store = StoreInterface(self.conn)
        self.user = UserInterface(self.conn)
        self.new_order = NewOrderInterface(self.conn)
        self.searcher = SearcherInterface(self.conn)
