import requests
from urllib.parse import urljoin
from fe.access import book
from typing import List, Dict, Any, Optional, Tuple, Union


class Searcher:
    def __init__(self, url_prefix):
        self.url_prefix = urljoin(url_prefix, "searcher/")

    def find_book(
        self,
        kind: str,
        dict_name: str,
        value: List[Union[str, int]],
        store_id: Optional[str],
        page_number: int = 1,
    ) -> Tuple[int, int, int, List[Tuple[str, str]]]:
        json = {
            "kind": kind,
            "store_id": store_id,
            "dict_name": dict_name,
            "value": value,
            "page_number": page_number,
        }
        url = urljoin(self.url_prefix, "find_book")
        r = requests.post(url, json=json)
        response_json = r.json()
        return (
            r.status_code,
            response_json.get("current_page"),
            response_json.get("total_page"),
            response_json.get("book_information"),
        )
