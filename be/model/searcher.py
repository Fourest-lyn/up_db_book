from . import error
from typing import List, Dict, Any, Optional, Tuple, Union
from .template.book_info import BookInfoTemp
from .db.interface import DBInterface


class Searcher:
    def __init__(self) -> None:
        self.db: DBInterface = DBInterface()
        self.page_size = 20

    def find_book_with_one_dict(
        self,
        dictName: str,
        value: Union[int, str],
        page: int,
        store_id: Optional[str] = None,
    ) -> Tuple[int, List[Tuple[str, str]]]:
        """
        Tuple[str,str] means (store_id, title)
        """
        if dictName is None or dictName == "" or value is None or value == "":
            raise BaseException
        st = (page - 1) * self.page_size + 1
        ed = page * self.page_size
        return_dict = ["store_id", "title"]
        number = self.db.searcher.find_book_with_one_dict_n(dictName, value, store_id)
        return_list = self.db.searcher.find_book_with_one_dict(
            dictName, value, st, ed, return_dict, store_id
        )
        # print(return_list)
        total_page = (number + self.page_size - 1) // self.page_size - 1
        return total_page, [(i["store_id"], i["title"]) for i in return_list]

    def find_book_with_content(
        self, content_piece: str, page: int, store_id: Optional[str] = None
    ) -> Tuple[int, List[Tuple[str, str]]]:
        if content_piece is None or content_piece == "":
            raise BaseException
        st = (page - 1) * self.page_size + 1
        ed = page * self.page_size
        return_dict = ["store_id", "title"]
        number = self.db.searcher.find_book_with_content_n(content_piece, store_id)
        return_list = self.db.searcher.find_book_with_content(
            content_piece, st, ed, return_dict, store_id
        )
        total_page = (number + self.page_size - 1) // self.page_size - 1
        return total_page, [(i["store_id"], i["title"]) for i in return_list]

    def find_book_with_tag(
        self, tags: List[str], page: int, store_id: Optional[str] = None
    ) -> Tuple[int, List[Tuple[str, str]]]:
        if tags is None or tags == []:
            raise BaseException
        st = (page - 1) * self.page_size + 1
        ed = page * self.page_size
        return_dict = ["store_id", "title"]
        number = self.db.searcher.find_book_with_tag_n(tags, store_id)
        return_list = self.db.searcher.find_book_with_tag(
            tags, st, ed, return_dict, store_id
        )
        total_page = (number + self.page_size - 1) // self.page_size - 1
        return total_page, [(i["store_id"], i["title"]) for i in return_list]
