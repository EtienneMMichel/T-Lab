from abc import ABC, abstractmethod
from enum import Enum

class AssetType(str, Enum):
    COIN = "Coin",
    STABLE_COIN = "StableCoin",
    DERIVATIVE = "Derivative",
    SPOT = "spot",
    FUTURE = "futures",
    PERPETUAL_FUTURE = "perpetual_futures",
    CALL_OPTION = "call_options",
    PUT_OPTION = "put_options"
    def __str__(self):
        return f"{self.value}"

class OrderType(str, Enum):
    MARKET_ORDER = "market_order",
    LIMIT_ORDER = "limit_order"

class OrderSide(str, Enum):
    BUY_ORDER = "buy",
    SELL_ORDER = "sell"

class Asset(ABC):
    @abstractmethod
    def __init__(self, platform_id, asset_symbol, asset_type, asset_id=None):
        self.platform_id = platform_id
        self.asset_symbol = asset_symbol
        self.asset_type = asset_type
        self.asset_id = asset_id if asset_id else asset_symbol
    
    def get_contract_size(self):
        if self.asset_symbol == 'BTC':
            contract_size = 0.001
        elif self.asset_symbol == 'ETH':
            contract_size = 0.01
        elif self.asset_symbol == 'BNB':
            contract_size = 0.1
        else:
            contract_size = 1
        return contract_size

class Derivative(Asset, ABC):
    @abstractmethod
    def __init__(self, platform_id, asset_symbol, asset_type, underlying_asset, currency, asset_id=None):
        super().__init__(platform_id, asset_symbol, asset_type, asset_id)
        self.underlying_asset = underlying_asset
        self.currency = currency

class Option(Derivative, ABC):
    @abstractmethod
    def __init__(self, platform_id, asset_symbol, asset_type, underlying_asset, currency, strike_price, settlement, asset_id=None):
        super().__init__(platform_id, asset_symbol, asset_type, underlying_asset, currency, asset_id)
        self.strike_price = strike_price
        self.settlement = settlement

class Coin(Asset):
    def __init__(self, platform_id, asset_symbol, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.COIN, asset_id)

class Spot(Derivative):
    def __init__(self, platform_id, asset_symbol, underlying_asset, currency, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.SPOT, underlying_asset, currency, asset_id)

class PerpetualFuture(Derivative):
    def __init__(self, platform_id, asset_symbol, underlying_asset, currency, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.PERPETUAL_FUTURE, underlying_asset, currency, asset_id)

class Future(Derivative):
    def __init__(self, platform_id, asset_symbol, underlying_asset, currency, settlement, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.FUTURE, underlying_asset, currency, asset_id)
        self.settlement = settlement

class CallOption(Option):
    def __init__(self, platform_id, asset_symbol, underlying_asset, currency, strike_price, settlement, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.CALL_OPTION, underlying_asset, currency, strike_price, settlement, asset_id)

class PutOption(Option):
    def __init__(self, platform_id, asset_symbol, underlying_asset, currency, strike_price, settlement, asset_id=None):
        super().__init__(platform_id, asset_symbol, AssetType.PUT_OPTION, underlying_asset, currency, strike_price, settlement, asset_id)

