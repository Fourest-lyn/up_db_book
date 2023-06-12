import requests
import threading
from urllib.parse import urljoin
from be import serve
from fe import conf
from typing import Optional
import pymongo

thread: Optional[threading.Thread] = None


def delete_database():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    client.drop_database("bookstore")
    assert "bookstore" not in client.list_database_names()


# 修改这里启动后端程序，如果不需要可删除这行代码
def run_backend():
    # rewrite this if rewrite backend
    serve.be_run()


def pytest_configure(config):
    delete_database()
    serve.be_init()

    global thread
    print("frontend begin test")
    thread = threading.Thread(target=run_backend)
    thread.start()


def pytest_unconfigure(config):
    url = urljoin(conf.URL, "shutdown")
    requests.get(url)
    thread.join()
    print("frontend end test")
