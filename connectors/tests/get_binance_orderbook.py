from connectors.crypto.connector.exchanges.binance import Binance


if __name__ == "__main__":
    connector = Binance("", "")
    orderbook = connector.get_orderbook("BNB_USDT")
    print(orderbook)
