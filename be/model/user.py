import jwt
import time
import logging
from pymongo.errors import DuplicateKeyError, PyMongoError
from . import error
from .db.interface import DBInterface
from .template.user_template import UserTemp
from typing import Tuple, Dict, Any

# encode a json string like:
#   {
#       "user_id": [user name],
#       "terminal": [terminal code],
#       "timestamp": [ts]} to a JWT
#   }


def jwt_encode(user_id: str, terminal: str) -> str:
    encoded = jwt.encode(
        {"user_id": user_id, "terminal": terminal, "timestamp": time.time()},
        key=user_id,
        algorithm="HS256",
    )
    return encoded.encode("utf-8").decode("utf-8")


# decode a JWT to a json string like:
#   {
#       "user_id": [user name],
#       "terminal": [terminal code],
#       "timestamp": [ts]} to a JWT
#   }
def jwt_decode(encoded_token, user_id: str) -> Dict[str, Any]:
    decoded = jwt.decode(encoded_token, key=user_id, algorithms=["HS256"])
    return decoded


class User:
    token_lifetime: int = 3600  # 3600 second

    def __init__(self):
        self.db: DBInterface = DBInterface()

    def __check_token(self, user_id, db_token, token) -> bool:
        try:
            if db_token != token:
                return False
            jwt_text = jwt_decode(encoded_token=token, user_id=user_id)
            ts = jwt_text["timestamp"]
            if ts is not None:
                now = time.time()
                if self.token_lifetime > now - ts >= 0:
                    return True
            return False  # TODO: check this Loc is right
        except jwt.exceptions.InvalidSignatureError as e:
            logging.error(str(e))
            return False

    def register(self, user_id: str, password: str):
        try:
            terminal = "terminal_{}".format(str(time.time()))
            token = jwt_encode(user_id, terminal)
            new_user = UserTemp(
                user_id=user_id,
                password=password,
                balance=0,
                token=token,
                terminal=terminal,
            )
            self.db.user.insert_one_user(new_user)
        except DuplicateKeyError:
            return error.error_exist_user_id(user_id)
        return 200, "ok"

    def check_token(self, user_id: str, token: str) -> Tuple[int, str]:
        db_token = self.db.user.get_token(user_id)
        if token is None:
            return error.error_authorization_fail()
        if not self.__check_token(user_id, db_token, token):
            return error.error_authorization_fail()
        return 200, "ok"

    def check_password(self, user_id: str, password: str) -> Tuple[int, str]:
        db_password = self.db.user.get_password(user_id)
        if db_password is None:
            return error.error_authorization_fail()

        if password != db_password:
            return error.error_authorization_fail()

        return 200, "ok"

    def login(self, user_id: str, password: str, terminal: str) -> Tuple[int, str, str]:
        token = ""
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message, ""

            token = jwt_encode(user_id, terminal)
            modified_count = self.db.user.update_token_terminal(
                user_id, token, terminal
            )
            if modified_count == 0:
                return error.error_authorization_fail() + ("",)
        except PyMongoError as e:
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            return 530, "{}".format(str(e)), ""
        return 200, "ok", token

    def logout(self, user_id: str, token: str) -> Tuple[int, str]:
        try:
            code, message = self.check_token(user_id, token)
            if code != 200:
                return code, message

            terminal = "terminal_{}".format(str(time.time()))
            dummy_token = jwt_encode(user_id, terminal)

            modified_count = self.db.user.update_token_terminal(
                user_id, dummy_token, terminal
            )
            if modified_count == 0:
                return error.error_authorization_fail()

        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def unregister(self, user_id: str, password: str) -> Tuple[int, str]:
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message

            deleted_count = self.db.user.delete_user(user_id)
            if deleted_count != 1:
                return error.error_authorization_fail()
        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def change_password(
        self, user_id: str, old_password: str, new_password: str
    ) -> Tuple[int, str]:
        try:
            code, message = self.check_password(user_id, old_password)
            if code != 200:
                return code, message

            terminal = "terminal_{}".format(str(time.time()))
            token = jwt_encode(user_id, terminal)
            modified_count = self.db.user.update_password(
                user_id=user_id,
                password=new_password,
                token=token,
                terminal=terminal,
            )
            if modified_count == 0:
                return error.error_authorization_fail()

        except PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"
