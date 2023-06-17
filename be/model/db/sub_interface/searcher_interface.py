from typing import List, Dict, Any, Optional, Union, Tuple
from pymongo.collection import Collection
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor
from ...template.book_info import BookInfoTemp
from ..db_client import DBClient
import re


class SearcherInterface:
    def __init__(self, conn: DBClient):
        self.conn: connection = conn.conn
        self.cur = self.conn.cursor()
        self.dcur = self.conn.cursor(cursor_factory=DictCursor)
        # self.storeCol: Collection = conn.storeCol
        # self.bookInfoCol: Collection = conn.bookInfoCol

    def get_one_info_by_info_id(
        self, info_id: str, dictName: str
    ) -> Optional[Union[str, int, List[str]]]:
        """
        This function will return a single information of match info_id
        """
        self.cur.execute(
            f"select {dictName} from book_info where book_info_id='{info_id}'"
        )
        result = self.cur.fetchone()[0]
        # result = self.bookInfoCol.find_one({"_id": info_id}, {"_id": 0, dictName: 1})
        # print(result)
        if result is None:
            return None
        else:
            return result

    def find_book_with_one_dict_n(
        self, dict_name: str, value: Union[int, str], store_id: Optional[str] = None
    ) -> int:
        if store_id is None:
            self.cur.execute(f"select * from book_info where {dict_name}= %s", (value,))
            result = len(self.cur.fetchall())
        else:
            sql = f"""
                select * 
                from book_info
                where {dict_name}= %s and store_id = %s
                """
            self.cur.execute(sql, (value, store_id))
            result = len(self.cur.fetchall())
        return result
        # return self.bookInfoCol.count_documents(query)

    def find_book_with_one_dict(
        self,
        dict_name: str,
        value: Union[int, str],
        st: int,
        ed: int,
        return_dict: List[str],
        store_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        This function could search books with one dict matching.
        Input:
            dict_name:  str
            value:      str/int
            st:         int
            ed:         int
            return_dict List[str]

        Output:
            List[Dict[str, Any]]

        The st and ed normally satisfy ed-st+1=20, and it will be 1-base
        Nomally we would require 'store_id' and 'book_id' and 'title' in return_dict
        The function returns some books with book[dictName]=value
        This function would not search by book_id
        """
        limit = ed - st + 1
        offset = st - 1
        select_columns = ", ".join(return_dict)
        if store_id is None:
            sql = f"SELECT {select_columns} FROM book_info WHERE {dict_name} = %s LIMIT %s OFFSET %s"
            self.cur.execute(sql, (value, limit, offset))
        else:
            query = f"""
                SELECT {select_columns}
                FROM book_info
                WHERE {dict_name} = %s AND store_id = %s
                LIMIT %s OFFSET %s
            """
            self.cur.execute(query, (value, store_id, limit, offset))

        # 获取查询结果并构造返回值
        results = self.cur.fetchall()
        return [dict(zip(return_dict, row)) for row in results]
        """
        query: Dict[str, Any]
        if store_id is None:
            query = {dict_name: value}
        else:
            query = {"store_id": store_id, dict_name: value}
        return_filter = {"_id": 0}
        for i in return_dict:
            return_filter[i] = 1
        # print(return_filter)
        result = (
            self.bookInfoCol.find(query, return_filter).skip(st - 1).limit(ed - st + 1)
        )
        return [i for i in result]
        """

    # This function is use for check the number of the book that meets the need.
    def find_book_with_content_n(
        self, content_piece: str, store_id: Optional[str] = None
    ) -> int:
        content_use= f"%{content_piece}%"
        if store_id is None:
            self.cur.execute(
                f"select * from book_info where content like %s", (content_use,)
            )
            result = len(self.cur.fetchall())
        else:
            sql = f"""
                select * 
                from book_info
                where content like %s and store_id = %s
                """
            self.cur.execute(sql, (content_use, store_id))
            result = len(self.cur.fetchall())
        return result

        """
        query: Dict[str, Any]
        assert len(content_piece) != 0
        content_piece = re.escape(content_piece)
        if store_id is None:
            query = {"content": {"$regex": content_piece}}
        else:
            query = {"store_id": store_id, "content": {"$regex": content_piece}}
        return self.bookInfoCol.count_documents(query)
        """

    def find_book_with_content(
        self,
        content_piece: str,
        st: int,
        ed: int,
        return_dict: List[str],
        store_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        This function returns a book_id which have a part of content_piece
        """
        limit = ed - st + 1
        offset = st - 1
        select_columns = ", ".join(return_dict)
        content_use= f"%{content_piece}%"
        if store_id is None:
            query = f"SELECT {select_columns} FROM book_info WHERE content like %s LIMIT %s OFFSET %s"
            self.cur.execute(query, (content_use, limit, offset))
        else:
            query = f"""
                SELECT {select_columns}
                FROM book_info
                WHERE content like %s AND store_id = %s
                LIMIT %s OFFSET %s
            """
            self.cur.execute(query, (content_use, store_id, limit, offset))
        results = self.cur.fetchall()
        return [dict(zip(return_dict, row)) for row in results]

        """
        query: Dict[str, Any]
        assert len(content_piece) != 0
        content_piece = re.escape(content_piece)
        if store_id is None:
            query = {"content": {"$regex": content_piece}}
        else:
            query = {
                "store_id": store_id,
                "content": {"$regex": content_piece},
            }
        return_filter = {"_id": 0}
        for i in return_dict:
            return_filter[i] = 1
        result = (
            self.bookInfoCol.find(query, return_filter).skip(st - 1).limit(ed - st + 1)
        )
        return [i for i in result]
        """

    def find_book_with_tag_n(
        self, tags: List[str], store_id: Optional[str] = None
    ) -> int:
        tag_string = ",".join(["%s"] * len(tags))
        if store_id is None:
            sql = f"""
                SELECT book_info_id
                FROM book_tags
                WHERE tag IN ({tag_string})
                GROUP BY book_info_id
                HAVING COUNT(DISTINCT tag) = %s
            """
            self.cur.execute(sql, (*tags, len(tags)))
            result = len(self.cur.fetchall())
        else:
            sql = f"""
                select * 
                from book_tags
                join book_list 
                on book_tags.book_info_id=book_list.book_info_id
                where book_tags.tag in ({tag_string}) and book_list.store_id = %s
                GROUP BY book_tags.book_info_id
                HAVING COUNT(DISTINCT tag) = %s
                """
            self.cur.execute(sql, (*tags, store_id, len(tags)))
            result = len(self.cur.fetchall())
        return result

        """
        query: Dict[str, Any]
        if store_id is None:
            query = {"tags": {"$all": tags}}
        else:
            query = {"store_id": store_id, "tags": {"$all": tags}}
        return self.bookInfoCol.count_documents(query)
        """

    def find_book_with_tag(
        self,
        tags: List[str],
        st: int,
        ed: int,
        return_dict: List[str],
        store_id: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        This function returns a book_id which have a tag
        """
        limit = ed - st + 1
        offset = st - 1
        select_columns = ", ".join(return_dict)
        group_columns = ", ".join(
            [f"book_tags.book_info_id"] + [f"book_info.{tp}" for tp in return_dict]
        )
        # tag_string = ",".join(["%s"] * len(tags))
        tag_string = ",".join([f"'{tag}'" for tag in tags])


        if store_id is None:
            sql = f"""
                SELECT {select_columns}
                FROM book_info
                JOIN book_tags
                ON book_tags.book_info_id = book_info.book_info_id
                WHERE book_tags.tag IN ({tag_string})
                GROUP BY {group_columns}
                HAVING COUNT(DISTINCT tag) = %s
                LIMIT %s OFFSET %s
            """
            self.cur.execute(sql, (len(tags), limit, offset))
        else:
            sql = f"""
                SELECT {select_columns}
                FROM book_tags
                JOIN book_list
                ON book_tags.book_info_id = book_list.book_info_id
                JOIN book_info
                ON book_tags.book_info_id = book_info.book_info_id
                WHERE tag IN ({tag_string}) AND book_list.store_id = %s
                GROUP BY {group_columns}
                HAVING COUNT(DISTINCT tag) = %s
                LIMIT %s OFFSET %s
                """
            self.cur.execute(sql, (store_id, len(tags), limit, offset))

        results = self.cur.fetchall()
        return [dict(zip(return_dict, row)) for row in results]

        """
        query: Dict[str, Any]
        if store_id is None:
            query = {"tags": {"$all": tags}}
        else:
            query = {"store_id": store_id, "tags": {"$all": tags}}
        return_filter = {"_id": 0}
        for i in return_dict:
            return_filter[i] = 1
        result = (
            self.bookInfoCol.find(query, return_filter).skip(st - 1).limit(ed - st + 1)
        )
        return [i for i in result]
        """
