from ..utils import Database

from enum import Enum
from enum import IntEnum

class Exchanges(IntEnum):
    BINANCE = 1
    DELTA_EXCHANGE = 2
    DERIBIT = 8
    COINBASE = 9
    KRAKEN = 10
    WOORTON = 11

class Rate(Enum):
    min_5 = 5
    min_10 = 10
    min_15 = 15
    hour_1 = 60
    hour_2 = 120
    hour_6 = 360
    hour_12 = 720
    day_1 = 1440
    