from connectors.crypto.common.logger import log
from connectors.crypto.exceptions import *

class UserNew:
    def __init__(self, user_id, database, use_testnet=False):
        self.id = user_id
        self.database = database

        # Initialize user information
        self.__user_df = self.database.getTable("users", arg=f"WHERE id = {self.id}")
        #if self.__user_df.empty:
        #    raise UnknownUserError(f"Unknown user {user_id}")
        self.firstname = self.__user_df["firstname"].values[0]
        self.lastname = self.__user_df["lastname"].values[0]

        self.email = self.__user_df["email"].values[0]
        self.testnet = use_testnet

        # Initialize user balances
        self.balances = {}

        # Initialize user payments

    def to_string(self):
        return "{} - {} {} ({})".format(self.id, self.firstname, self.lastname, self.email)

    def __str__(self):
        return f"{self.id}-{self.firstname}-{self.lastname}"



