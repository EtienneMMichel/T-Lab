"""
connector.py

Define abstract exchange connector class.

All classes implementing an exchange API connector shall inherit this class.

Output formats are documented bellow.
"""

from abc import ABC, abstractmethod, abstractproperty

class CryptoConnector(ABC):
    """
    CryptoConnector class

    Represent an abstract API connector class.

    Attributes
    ----------
        PLATFORM_ID: The database id of the platform.
        PLATFORM_NAME: The name of the platform.

    Methods
    -------
    TODO: List mandatory methods.
    """

    @abstractproperty
    def PLATFORM_ID(self):
        pass
    @abstractproperty
    def PLATFORM_NAME(self):
        pass

    @abstractmethod
    def __init__(self, api_key, api_secret, testnet=False, user=None, passphrase="", base_currency="USD"):
        """Create and returns the connector object.

        The connector is created from the user API keys.

        Parameters
        ----------
        api_key : str
            The user API key for the exchange.
        api_secret : str
            The user API secret for the exchange.
        testnet : bool
            Either the connector shall use the testnet endpoints or not. Defaults to 'False'.
        user : connectors.crypto.common.user.UserNew
            The user associated to the connector. Defaults to None.

        Returns
        -------
        CryptoConnector
            The CryptoConnector object instance.
        """
        pass

    @abstractmethod
    def is_ip_authorized(self):
        """Check whether is user has authorised the source IP address to connect to it's account.

        Returns
        -------
        bool
            Whether the source IP is authorized or not.
        """
        pass

    @abstractmethod
    def has_read_permission(self):
        """Check whether the read permission is enabled for this connector.

        Returns
        -------
        bool
            Whether the read permission is enabled or not.
        """
        pass

    @abstractmethod
    def has_write_permission(self):
        """Check whether the write permission is enabled for this connector.

        Returns
        -------
        bool
            Whether the write permission is enabled or not.
        """
        pass

    @abstractmethod
    def has_withdraw_permission(self):
        """Check whether the withdraw permission is enabled for this connector.

        Returns
        -------
        bool
            Whether the withdraw permission is enabled or not.
        """
        pass

    @abstractmethod
    def has_future_authorized(self):
        """Check whether the user can trade future in platform.

        Returns
        -------
        bool
            Whether the user can trade futures or not.
        """
        pass

    @abstractmethod
    def get_fees(self):
        """Return fees applied to trades.

        Returns
        -------
        dict
            The dict containing dicts of balance.

            Format
            -----
            {
                "taker": <taker>,
                "maker": <maker>,
            }

            Fields
            ------
            <taker>: float
                The fee rate applied for taker orders.
            <maker>: float
                The fee rate applied for maker orders.
        """
        pass

    @abstractmethod
    def get_balance(self, as_base_currency=False):
        """Retrieve balance on the exchange.

        The balance is returned for all supported currencies.

        Parameters
        ----------
        as_base_currency : bool
            If True, balances will be shown in connector base currency, otherwise into local currency.
            Defaults to False.

        Returns
        -------
        dict
            The dict containing dicts of balance.

            Format
            -----
            {
                <symbol>: {
                    "symbol": <symbol>,
                    "balance": <balance>,
                    "available_balance": <available_balance>,
                    "currency": <currency>,
                },
                ...
            }

            Fields
            ------
            <symbol>: str
                The symbol of the asset.
            <balance>: float
                The total balance on the exchange.
            <available_balance>: float
                The available balance on the exchange.
            <currency>: str
                The currency in which balances are shown.
        """
        pass

    @abstractmethod
    def get_profit(self):
        #TODO: Document this function
        pass

    @abstractmethod
    def get_products(self, reload=False):
        """Retrieve and returns all the available products on the exchange.
        All the products are saved into the self.products dictionnary for further access.
        Products are only retrieved once and then returned from the internal dictionnary.

        Parameters
        ----------
        reload : bool
            If True, force reload of products from the exchange.
            Defaults to False.

        Returns
        -------
        list
            The list containing dicts of products.

            Format
            -----
            [
                <symbol>: {
                    "symbol": <symbol>,
                    "exchange_id": <exchange_id>,
                    "exchange_symbol": <exchange_symbol>,
                    "contract_type": <contract_type>,
                    "contract_size": <contract_size>,
                    "strike_price": <strike_price>,
                    "settlement_date": <settlement_date>,
                    "settlement_time": <settlement_time>,
                    "duration": <duration>,
                    "precision": {
                        "amount": <precision_amount>,
                        "price": <precision_price>,
                        "notional": <precision_notional>,
                    },
                    "limits": {
                        "amount": {
                            "min": <limits_amount_min>
                            "max": <limits_amount_max>
                        },
                        "price": {
                            "min": <limits_price_min>
                            "max": <limits_price_max>
                        },
                        "notional": {
                            "min": <limits_notional_min>
                            "max": <limits_notional_max>
                        },
                    }
                    "base_asset_id": <base_asset_id>,
                    "base_asset_symbol": <base_asset_symbol>,
                    "quote_asset_id": <quote_asset_id>,
                    "quote_asset_symbol": <quote_asset_symbol>,
                },
                ...
            ]

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <contract_type>: Enum [spot|future|perpetual_future|call_option|put_option|move_option|spread|interest_rate_swap|staking]
                The type of the product.
            <contract_size>: float
                The deliverable quantity of the contract.
            <strike_price>: float
                The strike price associated to the product.
                None if not applicable.
            <settlement_date>: str
                The date at which the product expires as a string 'YYYY-MM-DD'.
                None if not applicable.
            <settlement_time>: str
                The date and time at which the product expires as a string 'YYYY-MM-DDTHH:MM:SSZ'.
                None if not applicable.
            <duration>: int
                The number of days until expiration.
                None if not applicable.
            <precision_amount>: float
                The precision of the amount for the product.
                None if not applicable.
            <precision_price>: float
                The precision of the price for the product.
                None if not applicable.
            <precision_notional>: float
                The precision of the amount for the product.
                None if not applicable.
            <limits_amount_min>: float
                The minimum size change when placing an order, size must be a multiple of it.
                None if not applicable.
            <limits_amount_max>: float
                The maximal size when placing an order, size must be a lower than it.
                None if not applicable.
            <limits_price_min>: float
                The minimum price change when placing an order, price must be a multiple of it.
                None if not applicable.
            <limits_price_max>: float
                The maximal price when placing an order, price must be lower than it.
                None if not applicable.
            <limits_notional_min>: float
                The minimum traded amount when placing an order, size*price must be greater than it.
                None if not applicable.
            <limits_notional_max>: float
                The maximal traded amount when placing an order.
                None if not applicable.
            <base_asset_id>: int, str
                The identifier of the base asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <base_asset_symbol>: str
                The symbol of the base asset on the exchange.
            <quote_asset_id>: int, str
                The identifier of the quoting asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <quote_asset_symbol>: str
                The symbol of the quoting asset on the exchange.
        """
        pass

    @abstractmethod
    def get_product(self, symbol):
        """Retrieve and returns the product identified by the given symbol.

        Parameters
        ----------
        symbol : str
            The symbol of the product.

        Returns
        -------
        dict
            The dictionnary describing the product.

            Format
            -----
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "contract_type": <contract_type>,
                "strike_price": <strike_price>,
                "settlement_date": <settlement_date>,
                "settlement_time": <settlement_time>,
                "tick_size": <tick_size>,
                "contract_size": <contract_size>,
                "base_asset_id": <base_asset_id>,
                "base_asset_symbol": <base_asset_symbol>,
                "quote_asset_id": <quote_asset_id>,
                "quote_asset_symbol": <quote_asset_symbol>,
            }

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <contract_type>: Enum [spot|future|perpetual_future|call_option|put_option|move_option|spread|interest_rate_swap]
                The type of the product.
            <strike_price>: float
                The strike price associated to the product.
                None if not applicable.
            <settlement_date>: str
                The date at which the product expires as a string 'YYYY-MM-DD'.
                None if not applicable.
            <settlement_time>: str
                The date and time at which the product expires as a string 'YYYY-MM-DDTHH:MM:SSZ'.
                None if not applicable.
            <tick_size>: float
                The minimum price change and thus, the number of decimals.
            <contract_size>: float
                The minimum contract size, when placing order, size must be a multiple of contract size.
            <base_asset_id>: int, str
                The identifier of the base asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <base_asset_symbol>: str
                The symbol of the base asset on the exchange.
            <quote_asset_id>: int, str
                The identifier of the quoting asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <quote_asset_symbol>: str
                The symbol of the quoting asset on the exchange.
        """
        pass

    @abstractmethod
    def get_orderbook(self, symbol):
        """Retrieve the mark_price of the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary representing the orderbook.

            Format
            ------
            {
                "buy": [
                    {
                        "price": <price>,
                        "size": <size>,
                    },
                    ...
                ],
                "sell": [
                    {
                        "price": <price>,
                        "size": <size>,
                    },
                    ...
                ],
                "symbol": <symbol>
            }

            Fields
            ------
            <price>: float
                The product buy or sell price.
            <size>: int
                The available size for the associated price.
            <symbol>: str
                The symbol of the product associated to the orderbook.
        """
        pass

    @abstractmethod
    def get_spot_price(self, symbol):
        """Retrieve the spot price associated to the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary containing the spot_price.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "spot_price": <spot_price>,
            }

            Fields
            ------
            <symbol>: str
                The symbol of the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <spot_price>: float
                The spot_price of the product.
        """
        pass

    @abstractmethod
    def get_mark_price(self, symbol):
        """Retrieve the mark price associated to the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary containing the mark_price.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "mark_price": <mark_price>,
            }

            Fields
            ------
            <symbol>: str
                The symbol of the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <mark_price>: float
                The mark_price of the product.
        """
        pass

    #@abstractmethod
    #TODO: Define as abstract and implement mock for binance connector
    def get_greeks(self, symbol):
        """Retrieve greeks associated to the given symbol.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary containing the greeks.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "mark_price": <mark_price>,
                "delta": <delta>,
                "gamma": <gamma>,
                "theta": <theta>,
                "rho": <rho>,
                "vega": <vega>,
            }

            Fields
            ------
            <symbol>: str
                The symbol of the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <delta>: float
                The delta of the product.
            <gamma>: float
                The gamma of the product.
            <theta>: float
                The theta of the product.
            <rho>: float
                The rho of the product.
            <vega>: float
                The vega of the product.
        """
        pass

    @abstractmethod
    def get_positions(self):
        """Retrieve the current positions on the exchange.

        Returns
        -------
        dict
            The dictionnary containing the positions.

            Format
            ------
            {
                <symbol>: {
                    "symbol": <symbol>,
                    "exchange_id": <exchange_id>,
                    "exchange_symbol": <exchange_symbol>,
                    "size": <size>,
                    "entry_price": <entry_price>,
                    "maintenance_margin": <maintenance_margin>,
                    "contract_type": <contract_type>,
                    "base_asset_id": <base_asset_id>,
                    "base_asset_symbol": <base_asset_symbol>,
                    "quote_asset_id": <quote_asset_id>,
                    "quote_asset_symbol": <quote_asset_symbol>,
                }
                ...
            }

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <size>: float
                The position size.
            <maintenance_margin>: float
                The maintenance margin associated to the position if applicable, otherwise set to 0.
            <contract_type>: Enum [spot|future|perpetual_future|call_option|put_option|move_option|spread|interest_rate_swap]
                The type of the product.
            <base_asset_id>: int, str
                The identifier of the base asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <base_asset_symbol>: str
                The symbol of the base asset on the exchange.
            <quote_asset_id>: int, str
                The identifier of the quoting asset on the exchange.
                Can be either an integer of a string depending on the exchange.
            <quote_asset_symbol>: str
                The symbol of the quoting asset on the exchange.
        """
        pass

    @abstractmethod
    def get_position(self, symbol):
        """Retrieve the position information on the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary containing the position information.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "size": <size>,
                "entry_price": <entry_price>,
            }

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <size>: int
                The position size.
            <entry_price>: float
                The price at which the product has been traded.
        """
        pass

    @abstractmethod
    def get_position_margin(self, symbol):
        """Retrieve the current margin on the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "auto_topup": <auto_topup>,
                "margin": <margin>,
                "entry_price": <entry_price>,
            }

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <auto_topup>: bool
                Whether the margin is auto-increased on margin call.
                None if not applicable.
            <margin>: float
                The margin amount on the position.
            <entry_price>: float
                The price at which the product has been traded.
        """
        pass

    @abstractmethod
    def set_position_margin(self, symbol, delta_margin):
        """Update the margin on the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        delta_margin: float
            The margin delta.
            Shall either be a positive number to add margin or a negative one to substract margin.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "auto_topup": <auto_topup>,
                "margin": <margin>,
                "entry_price": <entry_price>,
            }

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <auto_topup>: bool
                Whether the margin is auto-increased on margin call.
                None if not applicable.
            <margin>: float
                The updated margin amount on the position.
            <entry_price>: float
                The price at which the product has been traded.
        """
        pass

    @abstractmethod
    def place_market_order(self, symbol, size, side, time_in_force="gtc"):
        """Place a market order.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        size: float
            The size of the order.
            Some sizes may be restricted depending on the exchange.
        side: str
            The side of the order, either 'buy' or 'sell'.
        time_in_force: str
            How the order should be executed. Defaults to 'gtc'.
            Possible values:
                gtc (Good Til Cancelled): The order remains active whatever part is filled. Or is manually cancelled by the user.
                fok (Fill or Kill): The order shall be entirely filled, otherwise it's cancelled.
                ioc (Immediate or Cancel): The order shall fill any possible part, unfilled part is cancelled.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
                "commission": <commission>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The side of the order.
            <size>: int
                The size of the order when oppened.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
            <commission>: float
                The commission taken by the exchange on this order.
        """
        pass

    @abstractmethod
    def place_limit_order(self, symbol, size, side, limit_price, time_in_force="gtc", timeout=0):
        """Place a limit order.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        size: float
            The size of the order.
            Some sizes may be restricted depending on the exchange.
        side: str
            The side of the order, either 'buy' or 'sell'.
        limit_price: float
            The price at which the order shall be filled.
        time_in_force: str
            How the order should be executed. Defaults to 'gtc'.
            Possible values:
                gtc (Good Til Cancelled): The order remains active whatever part is filled. Or is manually cancelled by the user.
                fok (Fill or Kill): The order shall be entirely filled, otherwise it's cancelled.
                ioc (Immediate or Cancel): The order shall fill any possible part, unfilled part is cancelled.
        timeout: float
            The time to wait in seconds before cancelling order if not placed.
            If the timeout is zero or negative, the function won't wait for order execution.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
                "commission": <commission>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The side of the order.
            <size>: int
                The size of the order when oppened.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
            <commission>: float
                The commission taken by the exchange on this order.
        """
        pass

    @abstractmethod
    def place_stop_market_order(self, symbol, size, side, stop_price, time_in_force="gtc"):
        """Place a limit order.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        size: float
            The size of the order.
            Some sizes may be restricted depending on the exchange.
        side: str
            The side of the order, either 'buy' or 'sell'.
        stop_price: float
            The price at which the order shall be triggered.
        time_in_force: str
            How the order should be executed. Defaults to 'gtc'.
            Possible values:
                gtc (Good Til Cancelled): The order remains active whatever part is filled. Or is manually cancelled by the user.
                fok (Fill or Kill): The order shall be entirely filled, otherwise it's cancelled.
                ioc (Immediate or Cancel): The order shall fill any possible part, unfilled part is cancelled.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
                "commission": <commission>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The side of the order.
            <size>: int
                The size of the order when oppened.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
            <commission>: float
                The commission taken by the exchange on this order.
        """
        pass

    @abstractmethod
    def place_stop_limit_order(self, symbol, size, side, stop_price, limit_price, time_in_force="gtc"):
        """Place a limit order.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        size: float
            The size of the order.
            Some sizes may be restricted depending on the exchange.
        side: str
            The side of the order, either 'buy' or 'sell'.
        stop_price: float
            The price at which the order shall be triggered.
        limit_price: float
            The price at which the order shall be filled.
        time_in_force: str
            How the order should be executed. Defaults to 'gtc'.
            Possible values:
                gtc (Good Til Cancelled): The order remains active whatever part is filled. Or is manually cancelled by the user.
                fok (Fill or Kill): The order shall be entirely filled, otherwise it's cancelled.
                ioc (Immediate or Cancel): The order shall fill any possible part, unfilled part is cancelled.

        Returns
        -------
        dict
            The dictionnary containing the position margin.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
                "commission": <commission>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The side of the order.
            <size>: int
                The size of the order when oppened.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
            <commission>: float
                The commission taken by the exchange on this order.
        """
        pass

    @abstractmethod
    def get_active_orders(self, symbol=None):
        """Retrieve currently active orders on the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.

        Returns
        -------
        list
            The list containing the dictionnaries of orders.

            Format
            ------
            [
                {
                    "id": <order_id>,
                    "type": <type>,
                    "status": <status>,
                    "symbol": <symbol>,
                    "exchange_id": <exchange_id>,
                    "exchange_symbol": <exchange_symbol>,
                    "side": <side>,
                    "size": <size>,
                    "filled_size": <filled_size>,
                    "unfilled_size": <unfilled_size>,
                    "average_price": <average_price>,
                    "limit_price": <limit_price>,
                    "stop_price": <stop_price>,
                    "time_in_force": <time_in_force>,
                },
                ...
            ]

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The order side, either 'buy' or 'sell'.
            <size>: int
                The order size.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
        """
        pass

    def get_order_state(self, symbol, order_id):
        """Get the state of the order identified by the given id.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        order_id : str
            The id of the order on the exchange.

        Returns
        -------
        dict
            The dictionnary containing the order information.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The order side, either 'buy' or 'sell'.
            <size>: int
                The order size.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
        """
        pass

    @abstractmethod
    def cancel_order(self, symbol, order_id):
        """Cancel the given order on the given product.

        Parameters
        ----------
        symbol : str
            The unique symbol used to identify the product.
        order_id : str
            The id of the order on the exchange.

        Returns
        -------
        dict
            The dictionnary containing the order information.

            Format
            ------
            {
                "id": <order_id>,
                "type": <type>,
                "status": <status>,
                "symbol": <symbol>,
                "exchange_id": <exchange_id>,
                "exchange_symbol": <exchange_symbol>,
                "side": <side>,
                "size": <size>,
                "filled_size": <filled_size>,
                "unfilled_size": <unfilled_size>,
                "average_price": <average_price>,
                "limit_price": <limit_price>,
                "stop_price": <stop_price>,
                "time_in_force": <time_in_force>,
            }

            Fields
            ------
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The order side, either 'buy' or 'sell'.
            <size>: int
                The order size.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
        """
        pass

    @abstractmethod
    def get_order_history(self):
        """Returns the order history.

        Returns
        -------
        list
            The list containing dictionnaries of orders.

            Format
            ------
            [
                {
                    "created_at": <created_at>,
                    "updated_at": <updated_at>,
                    "id": <order_id>,
                    "type": <type>,
                    "status": <status>,
                    "symbol": <symbol>,
                    "exchange_id": <exchange_id>,
                    "exchange_symbol": <exchange_symbol>,
                    "side": <side>,
                    "size": <size>,
                    "filled_size": <filled_size>,
                    "unfilled_size": <unfilled_size>,
                    "average_price": <average_price>,
                    "limit_price": <limit_price>,
                    "stop_price": <stop_price>,
                    "time_in_force": <time_in_force>,
                    "source": <source>,
                    "is_liquidation": <is_liquidation>,
                    "cancellation_reason": <cancellation_reason>,
                }
            ]

            Fields
            ------
            <created_at>: str
                The date and time at which the order has been created as a string 'YYYY-MM-DDTHH:MM:SSZ'.
            <updated_at>: str
                The date and time at which the order has been updated as a string 'YYYY-MM-DDTHH:MM:SSZ'.
            <id>: str
                The id of the order on the exchange.
            <type>: Enum [market|limit|stop_market|stop_limit]
                The type of the order.
            <status>: Enum [open|filled|partially_filled|cancelled]
                The status of the order.
            <symbol>: str
                The unique symbol used to identify the product.
            <exchange_id>: int, str
                The identifier of the product on the exchange.
                Can be either an integer of a string depending on the exchange.
            <exchange_symbol>: str
                The symbol of the product on the exchange.
            <side>: Enum [buy|sell]
                The order side, either 'buy' or 'sell'.
            <size>: int
                The order size.
            <filled_size>: int
                The amount which has been filled.
            <unfilled_size>: int
                The amount which hasn't been filled.
            <average_price>: float
                The average execution price.
            <limit_price>: float
                The price at which the order has been filled.
                None if not applicable.
            <stop_price>: float
                The price at which the order has been triggered.
                None if not applicable.
            <time_in_force>: Enum [gtc|fok|ioc]
                How the order is executed. Refer to 'time_in_force' parameter.
            <source>: Enum [api|web]
                The source from which the order has been done.
                None if the order has been automatically done.
            <is_liquidation>: bool
                Whether the order is a liquidation or not.
            <cancellation_reason>: str
                The reason why order has been cancelled.
                None if not applicable.
        """
        pass

    @abstractmethod
    def get_leverage(self, symbol):
        """Returns the position leverage.

        Parameters
        ----------
        symbol : str
            The symbol of the position to get the leverage from.

        Returns
        -------
        dict
            The dict containing the position leverage.

            Format
            ------
            [
                {
                    "symbol": <symbol>,
                    "leverage": <leverage>,
                }
            ]

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <leverage>: int
                The positive integer representing the position leverage.
        """
        pass

    @abstractmethod
    def set_leverage(self, symbol, leverage):
        """Set the position leverage.

        Parameters
        ----------
        symbol : str
            The symbol of the position to set the leverage for.
        leverage : int
            The leverage value to set.

        Returns
        -------
        dict
            The dict containing the new position leverage.

            Format
            ------
            [
                {
                    "symbol": <symbol>,
                    "leverage": <leverage>,
                }
            ]

            Fields
            ------
            <symbol>: str
                The unique symbol used to identify the product.
            <leverage>: int
                The positive integer representing the position leverage.
        """
        pass

    @abstractmethod
    def get_deposit_withdraw_history(self, start_date=None, end_date=None, reload=False):
        """Retrieve deposit and withdraw history.

        Parameters
        ----------
        start_date : datetime.date
            The starting date of the history. If not given the first day of the curent year date will be used.
        end_date : datetime.date
            The ending date of the history. If not given, today's date will be used.

        Returns
        -------
        list
            The list containing deposits and withdraws.

            Format
            ------
            [
                {
                    "date": <date>,
                    "type": <type>,
                    "source_symbol": <source_symbol>,
                    "source_amount": <source_amount>,
                    "destination_symbol": <destination_symbol>,
                    "destination_amount": <destination_amount>,
                    "fees": <fees>,
                    "operation_id": <operation_id>,
                    "wallet_address": <wallet_address>,
                }
            ]

            Fields
            ------
            <date>: datetime.datetime
                The datetime at which the operation has been done.
            <type>: Enum [deposit|withdraw]
                The type of the operation.
            <source_symbol>: str
                The symbol of the source currency used for the operation.
            <source_amount>: float
                The amount of the source currency used for the operation.
            <destination_symbol>: str
                The symbol of the destination currency used for the operation.
            <destination_amount>: float
                The amount of the destination currency used for the operation.
            <fees>: float
                The fees associated to the operation.
            <operation_id>: str
                The id of the operation on the exchange.
            <wallet_address>: str
                The wallet_address used for the operation.
        """
        pass

    #TODO: Create uniformized model
    def create_wallet_address(self, symbol):
        raise NotImplementedError(f"Method 'create_wallet_address' not implemented for {self.__class__.__name__} connector")

    #TODO: Create uniformized model
    def get_wallet_address(self, symbol):
        raise NotImplementedError(f"Method 'get_wallet_address' not implemented for {self.__class__.__name__} connector")

    #TODO: Create uniformized model
    def withdraw(self, symbol, amount, address):
        raise NotImplementedError(f"Method 'withdraw' not implemented for {self.__class__.__name__} connector")

    @abstractmethod
    def is_connection_lost(self):
        """Tells if the websocket connection with the exchange has been lost.

        Returns
        -------
        bool
            Whether the websocket connection is lost or not.
        """
        pass

    @abstractmethod
    def is_connection_aborted(self):
        """Tells if the websocket connection with the exchange has been aborted. Meaning that no reconnction will be tried anymore.

        Returns
        -------
        bool
            Whether the websocket connection is aborted or not.
        """
        pass

    @abstractmethod
    def set_reconnect_timeout(self, timeout):
        """Set the timeout between two reconnection tries when connection is lost.

        Parameters
        ----------
        timeout : int
            The timeout in seconds.
        """
        pass

    @abstractmethod
    def set_abortion_datetime(self, datetime):
        """Set the datetime at which reconnection tries will be stopped. If not set, reconnections will be tried indefinitely.

        Parameters
        ----------
        datetime : datetime.datetime
            The datetime at which reconnection tries will be stopped.
            Set to 'None' for infinite reconnection tries.
        """
        pass

    @abstractmethod
    def show_websocket_logs(self, show):
        """Set whether websocket messages and events shall be shown or not.

        Returns
        ----------
        bool
            Whether websocket messages and events shall be shown or not.
        """
        pass

    @abstractmethod
    def cleanup(self):
        """Cleanup websocket if necessary.

        If channels are no longer subscribe calling this function will disconnect all the websockets.
        """
        pass

    @abstractmethod
    def subscribe_orderbook(self, symbols, callback=None):
        """Subscribe to orderbook updates through websocket.

        Parameters
        ----------
        symbols : str or list
            The symbols to subscribe to. If given as string, considered as a single symbol, otherwise as a list of symbol.
        callback : function(connector, data)
            The callback called on orderbook update event.

                connector : CryptoConnector
                    The connector from which the event has been triggered.
                data : dict
                    The data associated to the triggered event.

                Format
                ------
                {
                    "symbol": <symbol>,
                    "buy": [
                    {
                        "price": <price>,
                        "size": <size>,
                    },
                    ...
                    ],
                    "sell": [
                        {
                            "price": <price>,
                            "size": <size>,
                        },
                        ...
                    ],
                }

                Fields
                ------
                <symbol>: str
                    The symbol of the product associated to the orderbook.
                <price>: float
                    The product buy or sell price.
                <size>: int
                    The available size for the associated price.
        """
        pass

    @abstractmethod
    def subscribe_mark_price(self, symbols, callback=None):
        """Subscribe to mark price updates through websocket.

        Parameters
        ----------
        symbols : str or list
            The symbols to subscribe to. If given as string, considered as a single symbol, otherwise as a list of symbol.
        callback : function(connector, data)
            The callback called on mark price update event.

                connector : CryptoConnector
                    The connector from which the event has been triggered.
                data : dict
                    The data associated to the triggered event.

                Format
                ------
                {
                    "symbol": <symbol>,
                    "mark_price": <mark_price>,
                }

                Fields
                ------
                <symbol>: str
                    The symbol of the product associated to the orderbook.
                <mark_price>: float
                    The mark_price of the product.
        """
        pass

    @abstractmethod
    def subscribe_orders(self, symbols, callback=None):
        """Subscribe to order updates through websocket.

        Parameters
        ----------
        symbols : str or list
            The symbols to subscribe to. If given as string, considered as a single symbol, otherwise as a list of symbol.
        callback : function(connector, data)
            The callback called on order update event.

                connector : CryptoConnector
                    The connector from which the event has been triggered.
                data : dict
                    The data associated to the triggered event.

                Format
                ------
                {
                    "id": <order_id>,
                    "type": <type>,
                    "status": <status>,
                    "symbol": <symbol>,
                    "exchange_id": <exchange_id>,
                    "exchange_symbol": <exchange_symbol>,
                    "side": <side>,
                    "size": <size>,
                    "filled_size": <filled_size>,
                    "unfilled_size": <unfilled_size>,
                    "average_price": <average_price>,
                    "limit_price": <limit_price>,
                    "stop_price": <stop_price>,
                    "time_in_force": <time_in_force>,
                    "commission": <commission>,
                }

                Fields
                ------
                <id>: str
                    The id of the order on the exchange.
                <type>: Enum [market|limit|stop_market|stop_limit]
                    The type of the order.
                <status>: Enum [open|filled|partially_filled|cancelled]
                    The status of the order.
                <symbol>: str
                    The unique symbol used to identify the product.
                <exchange_id>: int, str
                    The identifier of the product on the exchange.
                    Can be either an integer of a string depending on the exchange.
                <exchange_symbol>: str
                    The symbol of the product on the exchange.
                <side>: Enum [buy|sell]
                    The side of the order.
                <size>: int
                    The size of the order when oppened.
                <filled_size>: int
                    The amount which has been filled.
                <unfilled_size>: int
                    The amount which hasn't been filled.
                <average_price>: float
                    The average execution price.
                <limit_price>: float
                    The price at which the order has been filled.
                    None if not applicable.
                <stop_price>: float
                    The price at which the order has been triggered.
                    None if not applicable.
                <time_in_force>: Enum [gtc|fok|ioc]
                    How the order is executed. Refer to 'time_in_force' parameter.
                <commission>: float
                    The commission taken by the exchange on this order.
        """
        pass

    def __str__(self):
        return f"{self.PLATFORM_ID}-{self.PLATFORM_NAME}"