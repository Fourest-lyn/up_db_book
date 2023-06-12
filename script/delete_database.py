import pymongo


def delete_database():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    client.drop_database("bookstore")
    assert "bookstore" not in client.list_database_names()


if __name__ == "__main__":
    delete_database()
