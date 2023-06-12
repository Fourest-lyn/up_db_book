from typing import Optional, List, Dict, Any


class UserTemp:
    def __init__(
        self,
        user_id: str,
        password: str,
        balance: int,
        token: str,
        terminal: str,
        order_id_list: List[str] = [],
    ):
        self.user_id: str = user_id
        self.password: str = password
        self.balance: int = balance
        self.token: str = token
        self.terminal: str = terminal
        self.order_id_list: List[str] = order_id_list

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "password": self.password,
            "balance": self.balance,
            "token": self.token,
            "terminal": self.terminal,
            "order_id_list": self.order_id_list,
        }

    @staticmethod
    def from_dict(data: dict) -> "UserTemp":
        return UserTemp(
            data["user_id"],
            data["password"],
            data["balance"],
            data["token"],
            data["terminal"],
            data["order_id_list"],
        )
