from flask import Blueprint
from flask import request
from flask import jsonify
from typing import List, Any, Tuple, Union, Optional
from be.model import searcher
from be.model import error
import logging

bp_searcher = Blueprint("searcher", __name__, url_prefix="/searcher")


@bp_searcher.route("/find_book", methods=["POST"])
def find_book() -> Any:
    """
    This function can find books with one dict
    Input:
        kind:           str
        store_id:       str
        dict_name:      str
        value:          str/int/List[str]
        page_number:    int

    Return:
        {page_number, current_page}, List[store_id, book_title], return_code

    the dict can search by this function include:
        id: str
        title: str
        author: str
        publisher: str
        original_title: str
        translator: str
        pub_year: str
        pages: int
        price: int
        currency_unit: int
        binding: str
        isbn: str
        author_intro: str
        book_intro: str
    We can only search for the book whose property is fully match on these dicts.
    The kind should be "one_dict","tags","content"
    The store_id could be None
    """
    assert request.json is not None
    code = 200
    # print("debug: reaquest.json=",request.json)
    kind: str = request.json.get("kind")
    store_id: Optional[str] = request.json.get("store_id")
    dict_name: Optional[str] = request.json.get("dict_name")
    value: Union[List[str], str, int] = request.json.get("value")
    page_number: int = request.json.get("page_number")

    s = searcher.Searcher()

    try:
        if kind == "one_dict":
            # print("debug: value=",value,"Type of value is",type(value))
            assert type(value) is int or type(value) is str
            assert type(dict_name) is str
            total_page, books = s.find_book_with_one_dict(
                dict_name, value, page_number, store_id
            )
        elif kind == "tags":
            assert type(value) is list and type(value[0]) is str
            total_page, books = s.find_book_with_tag(value, page_number, store_id)
        elif kind == "content":
            assert type(value) is str
            total_page, books = s.find_book_with_content(value, page_number, store_id)
        else:
            assert False
    except BaseException as e:
        logging.info("523, " + error.error_code[523])
        return 523, error.error_code[523]

    except:
        logging.info("522, {}".format(error.error_code[522]))
        return 522, error.error_code[522]

    return (
        jsonify(
            {
                "current_page": page_number,
                "total_page": total_page,
                "book_information": books,
            }
        ),
        code,
    )
