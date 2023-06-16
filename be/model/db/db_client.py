import psycopg2
from psycopg2.extensions import connection
from typing import Any, Optional

# import pymongo
# from pymongo import MongoClient
# from pymongo.database import Database
# from pymongo.collection import Collection
# from typing import Any, Optional


class DBClient:
    def __init__(
        self,
        host="localhost",
        port=5432,
        user="master",
        password="",
        database="postgres",
    ):
        self.host: str = host
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.conn: connection = psycopg2.connect(
            host=host, port=port, user=user, password=password, database=database
        )
    

    def database_init(self) -> None:
        with self.conn.cursor() as cursor:
            # user's order can be find in the table "new_order", so we don't save order_list in "users"
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id text unique,
                    password text,
                    balance int,
                    token text,
                    terminal text
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stores (
                    store_id text PRIMARY KEY,
                    user_id text
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS book_list(
                    store_id text,
                    book_id text,
                    book_info_id text,
                    stock_level int
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS book_info (
                    book_info_id serial PRIMARY KEY,
                    book_id text,
                    id text,
                    title text,
                    author text,
                    publisher text,
                    original_title text,
                    translator text,
                    pub_year text,
                    pages int,
                    price int,
                    currency_unit text,
                    binding text,
                    isbn text,
                    author_intro text,
                    book_intro text,
                    content text
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS book_tags (
                    book_info_id text,
                    tag text
                )
                """
            )
            # there is book list in order_id, but we temporarily ignore it
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS new_order (
                    order_id text PRIMARY KEY,
                    user_id text ,
                    store_id text,
                    create_time int,
                    status int
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS order_book (
                    order_id text ,
                    book_id text,
                    count int,
                    price int
                )
                """
            )
        self.conn.commit()

    def database_reset(self) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS users")
            cursor.execute("DROP TABLE IF EXISTS stores")
            cursor.execute("DROP TABLE IF EXISTS book_list")
            cursor.execute("DROP TABLE IF EXISTS book_info")
            cursor.execute("DROP TABLE IF EXISTS book_tags")
            cursor.execute("DROP TABLE IF EXISTS new_order")
            cursor.execute("DROP TABLE IF EXISTS order_book")
        self.conn.commit()
        self.database_init()

    def database_close(self) -> None:
        self.conn.close()


# database_instance: Optional[DBClient] = None


"""
class DBClient:
    def __init__(self, connectUrl="mongodb://localhost:27017/", database="bookstore"):
        self.connectUrl: str = connectUrl
        self.database: str = database
        self.client: MongoClient = pymongo.MongoClient(connectUrl)

    def database_init(self) -> None:
        self.db: Database = self.client[self.database]
        self.userCol: Collection[Any] = self.db["user"]
        self.userCol.create_index([("user_id", 1)], unique=True)
        self.storeCol: Collection[Any] = self.db["store"]
        self.storeCol.create_index(
            [("store_id", 1), ("book_list.book_id", 1)], unique=True
        )
        self.bookInfoCol: Collection[Any] = self.db["book_info"]
        self.bookInfoCol.create_index([("tags", 1)])
        self.newOrderCol: Collection[Any] = self.db["new_order"]
        self.newOrderCol.create_index([("order_id", 1)], unique=True)

    def database_reset(self) -> None:
        self.client.drop_database(self.database)
        self.database_init()

    def database_close(self) -> None:
        self.client.close()
"""

database_instance: Optional[DBClient] = None


def db_init() -> None:
    global database_instance
    database_instance = DBClient()
    database_instance.database_init()


def get_db_conn() -> DBClient:
    global database_instance
    assert database_instance is not None, "Database not initialized"
    return database_instance


db_column_list = {
    "users": ("user_id", "password", "balance", "token", "terminal"),
    "stores": ("store_id", "user_id"),
    "book_list": ("store_id", "book_id", "book_info_id", "stock_level"),
    "book_info": ("book_info_id",),  # maybe we need not use this line
    "book_tags": ("book_info_id","tag"),
    "new_order": ("order_id", "user_id", "store_id", "create_time", "status"),
    "order_book": ("order_id", "book_id", "count", "price"),
}
