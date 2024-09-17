
from connectors.crypto.connector.exchanges.delta_exchange import DeltaExchange
from connectors.crypto.connector.exchanges.binance import Binance
from connectors.crypto.connector.exchanges.deribit import Deribit
from connectors.crypto.connector.exchanges.woorton import Woorton
from connectors.crypto.connector.exchanges.coinbase import Coinbase
from connectors.crypto.connector.exchanges.kraken import Kraken
from enum import IntEnum

class Exchanges(IntEnum):
    BINANCE = 1
    DELTA_EXCHANGE = 2
    DERIBIT = 8
    COINBASE = 9
    KRAKEN = 10
    WOORTON = 11

    def __hash__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, int):
            if int(self.value) == other:
                return True
        elif isinstance(other, str):
            if str(self.value) == other:
                return True
        return False

    def __str__(self):
        return str(self.value)

class Exchange:

    def __init__(self, platform_id):
        self.platform_id = platform_id
        self.__fireblocks_account = None
        if platform_id is None:
            self.name = "Unknown"
            self.connector = None
        elif int(platform_id) == Exchanges.BINANCE:
            self.name = "Binance"
            self.connector = Binance
        elif int(platform_id) == Exchanges.DELTA_EXCHANGE:
            self.name = "Delta Exchange"
            self.connector = DeltaExchange
        elif int(platform_id) == Exchanges.DERIBIT:
            self.name = "Deribit"
            self.connector = Deribit
        elif int(platform_id) == Exchanges.KRAKEN:
            self.name = "Kraken"
            self.connector = Kraken
        elif int(platform_id) == Exchanges.COINBASE:
            self.name = "Coinbase"
            self.connector = Coinbase
        elif int(platform_id) == Exchanges.KRAKEN:
            self.name = "Kraken"
            self.connector = Kraken
        elif int(platform_id) == Exchanges.WOORTON:
            self.name = "Woorton"
            self.connector = Woorton
        else:
            self.name = f"Undefined Exchange identified by '{platform_id}'"
            self.connector = None

    def get_fireblocks_account(self, database):
        if not self.__fireblocks_account:
            platform_df = database.getTable("crypto_platforms ", arg=f"WHERE id = {self.platform_id}")
            if not platform_df.empty:
                self.__fireblocks_account = platform_df["fireblocks_account"].values[0]
        return self.__fireblocks_account

    def __eq__(self, other):
        if isinstance(other, Exchange):
            return self.platform_id == other.platform_id
        else:
            return self.platform_id == other

    def __str__(self):
        return self.name
