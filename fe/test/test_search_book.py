import pytest

from fe.test.gen_book_data import GenBook
from fe.access.searcher import Searcher
from fe.access import book
from fe import conf
import uuid, random

from fe.bench.workload import Workload
from fe.access.new_seller import register_new_seller
from fe.access.seller import Seller
from typing import List, Tuple


class TestSearchBook:
    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        # wl = Workload()
        # wl.gen_database()

        self.searcher = Searcher(conf.URL)
        self.seller_id = "test_add_books_seller_id_{}".format(str(uuid.uuid1()))
        self.password = ""
        self.seller = register_new_seller(self.seller_id, self.password)

        # seller process
        self.store_number = 5
        # self.store_list: List[Seller] = []
        self.store_id_list = []

        for i in range(self.store_number):
            new_store_id = "test_add_books_store_id_{}".format(str(uuid.uuid1()))
            code = self.seller.create_store(new_store_id)
            # self.store_list.append(new_seller)
            self.store_id_list.append(new_store_id)

        # generate books
        self.book_db = book.BookDB()
        rows = self.book_db.get_book_count()
        start = 0
        max_book_count = 1000
        if rows > max_book_count:
            start = random.randint(0, rows - max_book_count)
        size = random.randint(1, max_book_count)
        self.books = self.book_db.get_book_info(start, size)

        # put books into sellers
        for bk in range(len(self.books)):
            cu_store_id = self.store_id_list[bk % self.store_number]
            code = self.seller.add_book(cu_store_id, 1, self.books[bk])
            assert code == 200

        yield

    def test_search_with_title(self):
        for i in self.books:
            target_title = i.title
            # get answer from self.books
            answer_list: List[str] = []  # only store_id list
            for j in range(len(self.books)):
                if self.books[j].title == target_title:
                    answer_list.append(self.store_id_list[j % self.store_number])

            # get our answer for request
            cu_page = 0
            total_page = 1
            check_list = []
            while cu_page <= total_page:
                cu_page = cu_page + 1
                code, cu_page, total_page, temp_list = self.searcher.find_book(
                    "one_dict", "title", target_title, None, cu_page
                )
                assert code == 200
                for k in temp_list:
                    # filter for data generate by other test
                    if k[0] in self.store_id_list:
                        check_list.append(k[0])

            # compare answer_list and check_list
            ans = set(answer_list)
            chk = set(check_list)
            assert ans == chk

    def test_search_with_author(self):
        for i in self.books:
            if i.author is None:
                continue
            target_author = i.author
            # get answer from self.books
            answer_list: List[Tuple[str, str]] = []  # only store_id list
            for j in range(len(self.books)):
                if self.books[j].author == target_author:
                    answer_list.append(
                        (
                            self.store_id_list[j % self.store_number],
                            self.books[j].title,
                        )
                    )

            # get our answer for request
            cu_page = 0
            total_page = 1
            check_list: List[Tuple[str, str]] = []
            while cu_page <= total_page:
                cu_page = cu_page + 1
                code, cu_page, total_page, temp_list = self.searcher.find_book(
                    "one_dict", "author", target_author, None, cu_page
                )
                assert code == 200
                for k in temp_list:
                    # filter for data generate by other test
                    if k[0] in self.store_id_list:
                        check_list.append(k)

            # compare answer_list and check_list

            def change_st(temp_list: List[List[str]]) -> List[Tuple[str, str]]:
                return_list = []
                for i in temp_list:
                    return_list.append((i[0], i[1]))
                return return_list

            ans = set(answer_list)
            chk = set(change_st(check_list))
            assert ans == chk

    def test_search_with_tags(self):
        for i in self.books:
            if len(i.tags) > 1:
                target_tags = i.tags[:2]
            elif len(i.tags) == 0:
                continue
            else:
                target_tags = i.tags
            # get answer from self.books
            answer_list: List[Tuple[str, str]] = []  # only store_id list
            for j in range(len(self.books)):
                if set(target_tags) <= set(self.books[j].tags):
                    answer_list.append(
                        (
                            self.store_id_list[j % self.store_number],
                            self.books[j].title,
                        )
                    )

            # get our answer for request
            cu_page = 0
            total_page = 1
            check_list: List[Tuple[str, str]] = []
            while cu_page <= total_page:
                cu_page = cu_page + 1
                code, cu_page, total_page, temp_list = self.searcher.find_book(
                    "tags", None, target_tags, None, cu_page
                )
                assert code == 200
                for k in temp_list:
                    # filter for data generate by other test
                    if k[0] in self.store_id_list:
                        check_list.append(k)

            # compare answer_list and check_list
            def change_st(temp_list: List[List[str]]) -> List[Tuple[str, str]]:
                return_list = []
                for i in temp_list:
                    return_list.append((i[0], i[1]))
                return return_list

            ans = set(answer_list)
            chk = set(change_st(check_list))
            assert ans == chk

    def test_search_with_contents(self):
        for i in self.books:
            if len(i.content) > 1:
                cut_len = 10
                start = random.randint(0, len(i.content) - cut_len)
                endd = random.randint(start, start + cut_len)
                if start == endd:
                    endd = endd + 1
                target_contents = i.content[start:endd]
            elif i.content is None or len(i.content) == 0:
                continue
            else:
                target_contents = i.content
            assert len(target_contents) > 0
            # get answer from self.books
            answer_list: List[Tuple[str, str]] = []  # only store_id list
            for j in range(len(self.books)):
                if target_contents in self.books[j].content:
                    answer_list.append(
                        (
                            self.store_id_list[j % self.store_number],
                            self.books[j].title,
                        )
                    )

            # get our answer for request
            cu_page = 0
            total_page = 1
            check_list: List[Tuple[str, str]] = []
            while cu_page <= total_page:
                cu_page = cu_page + 1
                code, cu_page, total_page, temp_list = self.searcher.find_book(
                    "content", None, target_contents, None, cu_page
                )
                assert code == 200
                for k in temp_list:
                    # filter for data generate by other test
                    if k[0] in self.store_id_list:
                        check_list.append((k[0], k[1]))

            # compare answer_list and check_list

            print("answer_list=", answer_list)
            print("check_list=", check_list)
            ans = set(answer_list)
            chk = set(check_list)
            assert ans == chk

    def test_search_with_author_with_store_id(self):
        for i in range(len(self.books)):
            if self.books[i].author is None:
                continue
            target_author = self.books[i].author
            target_store_id = self.store_id_list[i % self.store_number]

            # get answer from self.books
            answer_list: List[Tuple[str, str]] = []  # only store_id list
            for j in range(len(self.books)):
                if (
                    self.books[j].author == target_author
                    and i % self.store_number == j % self.store_number
                ):
                    answer_list.append(
                        (
                            self.store_id_list[j % self.store_number],
                            self.books[j].title,
                        )
                    )

            # get our answer for request
            cu_page = 0
            total_page = 1
            check_list: List[Tuple[str, str]] = []
            while cu_page <= total_page:
                cu_page = cu_page + 1
                code, cu_page, total_page, temp_list = self.searcher.find_book(
                    "one_dict", "author", target_author, target_store_id, cu_page
                )
                assert code == 200
                for k in temp_list:
                    # filter for data generate by other test
                    if k[0] in self.store_id_list:
                        check_list.append((k[0], k[1]))

            # compare answer_list and check_list
            ans = set(answer_list)
            chk = set(check_list)
            assert ans == chk
