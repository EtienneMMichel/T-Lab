from connectors.crypto.connector.common.platforms import Exchanges, Exchange
from connectors.crypto.connector.strategy_connector import StrategyConnector
from connectors.crypto.connector.exchanges.coinbase import Coinbase, CbForex
from connectors.crypto.connector.binance_link import BinanceLinkTransfer
from connectors.crypto.connector.fireblocks import FireblocksTransfer
from connectors.crypto.connector.defi_connector import DefiConnector
from connectors.crypto.connector.mev_capital import MevCapital
from connectors.threading.Threads import format_traceback
from connectors.crypto.common.logger import log
from connectors.crypto.connector.core import *
from connectors.crypto.exceptions import *
import connectors.config as connectors_config
from enum import IntEnum
import pandas as pd
import binascii
import datetime
import random
import string
import time
import json
import sys
import os

class Groupement:
    def __init__(self, groupement_id, database):
        self.id = groupement_id
        self.database = database

        self.__groupement_df = self.database.getTable("groupements", arg=f"WHERE id = {self.id}")
        self.tilvest_management_cut = self.__groupement_df["tilvest_management_cut"].values[0]
        self.tilvest_custodian_cut = self.__groupement_df["tilvest_custodian_cut"].values[0]
        self.tilvest_operation_cut = self.__groupement_df["tilvest_operation_cut"].values[0]


    def get_distributors(self):
        self.distributors = []
        distributors_df = self.database.getTable("distributors", arg=f"WHERE groupement_id = {self.id}")
        for index, distrib in distributors_df.iterrows():
            distributor = Distributor(distrib.id, self.database)
            self.distributors.append(distributor)
        return self.distributors


class Distributor:
    def __init__(self, distributor_id, database):
        self.id = distributor_id
        self.database = database

        self.__distributor_df = self.database.getTable("distributors", arg=f"WHERE id = {self.id}")
        self.webapp_id = self.__distributor_df["webapp_id"].values[0]
        self.is_payment_euro = self.__distributor_df["is_payment_euro"].values[0]
        self.is_groupement = self.__distributor_df["is_groupement"].values[0]
        self.groupement_management_cut = self.__distributor_df["groupement_management_cut"].values[0]
        self.groupement_custodian_cut = self.__distributor_df["groupement_custodian_cut"].values[0]
        self.groupement_operation_cut  = self.__distributor_df["groupement_operation_cut"].values[0]

        self.groupement_part = True if self.__distributor_df["groupement_id"].values[0] else False
        #if self.groupement_part:
        self.groupement_id = self.__distributor_df["groupement_id"].values[0]

        self.is_harvest_distributor = True if self.__distributor_df["is_harvest_distributor"].values[0] else False
        self.is_karbon_alpha_distributor = True if self.__distributor_df["is_karbon_alpha_distributor"].values[0] else False
        self.is_wealthcome_distributor = True if self.__distributor_df["is_wealthcome_distributor"].values[0] else False

    def get_strategies(self):
        return self.strategies

    def get_clients(self):
        self.clients = []
        clients_df = self.database.getTable("clients", arg=f"WHERE distributor_id = {self.id}")
        for index, client in clients_df.iterrows():
            client = Client(client.id, self.database)
            self.clients.append(client)
        return self.clients

    def __str__(self):
        return f"{self.id}-{self.webapp_id}"

class Client:
    def __init__(self, client_id, database):
        self.id = client_id
        self.database = database

        self.__client_df = self.database.getTable("clients", arg=f"WHERE id = {self.id}")
        self.user_balances = self.database.getTable("users_balances")
        self.__client_shares_df = self.database.getTable("user_strategy_shares", arg=f"WHERE client_id = {self.id}")
        self.__client_euro_cashflows_df = self.database.getTable("client_euro_cashflows", arg=f"WHERE client_id = {self.id} AND tilvest_received_coinbase = 1 AND tilvest_transferred_coinbase=1")
        self.__client_euro_arbitrages_df = self.database.getTable("client_euro_arbitrages", arg=f"WHERE client_id = {self.id} AND transaction_withdrawn = 1 AND transaction_deposited=1")
        self.webapp_id = self.__client_df["webapp_id"].values[0]
        self.distributor_id = self.__client_df["distributor_id"].values[0]
        self.balances = {}
        self.profit = {}

        self.strategies = self._initialize_strategies()

    def get_strategies(self):
        return self.strategies

    def get_balance(self, strategy):
        self.balances = self.__update_balances(strategy)
        return self.balances

    def __update_balances(self, strategy):
        print('strategy',strategy.id)
        client_strategy_shares = self.__client_shares_df[self.__client_shares_df['strategy_id'] ==strategy.id]['number_shares'].values[-1]
        client_total_deposit_net = self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] ==strategy.id) & (self.__client_euro_cashflows_df['transaction_type'] =='deposit')]['amount_eur_net'].sum()
        client_total_deposit_gross = self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] ==strategy.id) & (self.__client_euro_cashflows_df['transaction_type'] =='deposit')]['amount_eur_gross'].sum()
        client_total_withdraw = self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] ==strategy.id) & (self.__client_euro_cashflows_df['transaction_type'] =='withdraw')]['amount_eur_net'].sum()

        client_total_deposit_net += self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['to_strategy_id'] ==strategy.id]['amount_eur_net'].sum()
        client_total_deposit_gross += self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['to_strategy_id'] ==strategy.id]['amount_eur_gross'].sum()
        client_total_withdraw += self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['from_strategy_id'] ==strategy.id]['amount_eur_net'].sum()

        # To reconstruct history data
        #strat_nav = float(self.user_balances_history[(self.user_balances_history['strategy_id'] == strategy.id) & (self.user_balances_history["date"] == datetime.date(2023, 10, 26))]['nav'].values[0])/CbForex(self.database).get_rate("EUR",  strategy.base_currency)
        strat_nav = float(self.user_balances[self.user_balances['strategy_id'] == strategy.id]['nav'].values[-1])/CbForex(self.database).get_rate("EUR",  strategy.base_currency)

        last_hwm = round(float(self.user_balances[self.user_balances["strategy_id"] == strategy.id]['last_hwm'].values[-1]), 2)

        client_total_balance = client_strategy_shares*strat_nav

        print('client_strategy_shares',client_strategy_shares)
        print('strat_nav',strat_nav)
        print('client_total_balance',client_total_balance)
        print('client_total_deposit_net',client_total_deposit_net)
        print('client_total_withdraw',client_total_withdraw)

        total_profit = client_total_balance - client_total_deposit_net + client_total_withdraw

        client_profit = {
                "total_balance": client_total_balance,
                "total_deposit_net": client_total_deposit_net,
                "total_deposit_gross": client_total_deposit_gross,
                "total_withdraw": client_total_withdraw,
                "total_profit": total_profit,
                "nav": strat_nav,
                "last_hwm": last_hwm,
                "client_strategy_shares": client_strategy_shares
            }

        return client_profit

    def _initialize_strategies(self):

        strategies = []

        unique_strategies = []
        # traverse for all elements
        for strategy_id in self.__client_euro_cashflows_df['strategy_id']:
            # check if exists in unique_list or not
            if strategy_id not in unique_strategies:
                unique_strategies.append(strategy_id)

        for strategy_id in self.__client_euro_arbitrages_df['to_strategy_id']:
            # check if exists in unique_list or not
            if strategy_id not in unique_strategies:
                unique_strategies.append(strategy_id)

        for strategy_id in self.__client_euro_arbitrages_df['from_strategy_id']:
            # check if exists in unique_list or not
            if strategy_id not in unique_strategies:
                unique_strategies.append(strategy_id)

        for strategy_id in unique_strategies:
            total_deposit_net = 0
            total_withdraw = 0

            if len(self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] == strategy_id) & (self.__client_euro_cashflows_df['transaction_type'] == 'deposit')]) > 0:
                total_deposit_net += self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] == strategy_id) & (self.__client_euro_cashflows_df['transaction_type'] == 'deposit')]['amount_eur_net'].sum()
            if len(self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] == strategy_id) & (self.__client_euro_cashflows_df['transaction_type'] == 'withdraw')]) > 0:
                total_withdraw += self.__client_euro_cashflows_df[(self.__client_euro_cashflows_df['strategy_id'] == strategy_id) & (self.__client_euro_cashflows_df['transaction_type'] == 'withdraw')]['amount_eur_net'].sum()

            if len(self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['to_strategy_id'] == strategy_id]) > 0:
                total_deposit_net += self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['to_strategy_id'] == strategy_id]['amount_eur_net'].sum()
            if len(self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['from_strategy_id'] == strategy_id]) > 0:
                total_withdraw += self.__client_euro_arbitrages_df[self.__client_euro_arbitrages_df['from_strategy_id'] == strategy_id]['amount_eur_net'].sum()

            if float(total_deposit_net) > float(total_withdraw):
                #still money in the strategy
                strategy = Strategy(strategy_id, self.database, self)
                if not strategy.is_disabled():
                    strategies.append(strategy)

        return strategies

    def __str__(self):
        return f"{self.id}-{self.webapp_id}"


class AssetManager:
    def __init__(self, asset_manager_id, database):
        self.id = asset_manager_id
        self.database = database

        self.__asset_manager_df = self.database.getTable("asset_managers", arg=f"WHERE id = {self.id}")
        self.name = self.__asset_manager_df["name"].values[0]
        self.__user_df = self.database.getTable("users", arg=f"WHERE asset_manager_id = {self.id} AND manager = 1")
        self.email = self.__user_df["email"].values[0] if not self.__user_df.empty else None
        self.is_payment_euro = True if self.__asset_manager_df["is_payment_euro"].values[0] else False
        self.is_custodian = True if self.__asset_manager_df["is_custodian"].values[0] else False
        self.balances = {}
        self.profit = {}

        self.strategies = self._initialize_strategies()

    def get_strategies(self):
        return self.strategies

    def get_balance(self, reload=False):
        self.__update_balances(reload)
        return self.balances

    def __update_balances(self, reload=False):
        if self.balances and not reload:
            return self.balances
        else:
            for strategy in self.strategies:
                self.balances[strategy.id] = strategy.get_balance(reload)
            return self.balances

    def get_profit(self, reload=False):
        self.__update_profit(reload)
        return self.profit

    def __update_profit(self, reload=False):
        if self.profit and not reload:
            return self.profit
        else:
            for strategy in self.strategies:
                self.profit[strategy.id] = strategy.get_profit(reload)
            return self.profit

    def compute_delta_platforms(self, asset_manager_strategies, am_positions, am_positions_delta, am_global_delta, asset_manager):
        for strategy_id in asset_manager_strategies:

            am_positions[strategy_id] = {}
            am_global_delta[strategy_id] = {}
            am_positions_delta[strategy_id] = {}

            strategy = Strategy(strategy_id, self.database, asset_manager)

            platform_positions = strategy.get_positions()
            if strategy.type =='cefi':
                log(f"[{self}] Platform {strategy.platform_id} positions :  {platform_positions}")
            for symbol, position in platform_positions.items():
                delta = 0
                gamma = 0
                vega = 0
                theta = 0

                if strategy.platform_id == Exchanges.DELTA_EXCHANGE or strategy.platform_id == Exchanges.DELTA_EXCHANGE_MARKET_MAKER:
                    contract_size = get_contract_size(position['base_asset_symbol'])
                    size = float(position['size'])*contract_size
                else:
                    size = float(position['size'])

                if is_fiat(symbol) or is_stable_coin(symbol) or symbol == 'DETO_USDT' or symbol =='ETHW' or symbol =='LUNA' or symbol =='LUNC' or symbol =='LDBTC' or symbol=='BETH' or symbol=='LDNEAR':
                    mark_price = 1
                else:
                    try:
                        mark_price = strategy.get_api_connector().get_mark_price(position['base_asset_symbol']+'_PERP_USDT')['mark_price']
                    except:
                        try:
                            mark_price = strategy.get_api_connector().get_mark_price(position['base_asset_symbol']+'_PERP_USDT')['mark_price']
                        except:
                            try:
                                mark_price = strategy.get_api_connector().get_mark_price(position['base_asset_symbol']+'_USDT')['mark_price']
                            except:
                                print(position['base_asset_symbol'])
                                mark_price = strategy.get_api_connector().get_mark_price(position['base_asset_symbol']+'_BUSD')['mark_price']

                if position['contract_type'] == 'call_option' or position['contract_type'] == 'put_option':
                    if position['symbol'][-3:] == 'BTC' or position['symbol'][-3:] == 'SOL' or position['symbol'][-3:] == 'ETH':
                        try:
                            greeks = strategy.get_api_connector().get_greeks(position['symbol'])
                            delta = greeks['delta']
                            gamma = greeks['gamma']
                            vega = greeks['vega']
                            theta = greeks['theta']
                        except:
                            pass

                    position['delta'] = float(delta)*size
                    position['gamma'] = float(gamma)*size
                    position['vega'] = float(vega)*size
                    position['theta'] = float(theta)*size

                else:
                    if strategy.platform_id == Exchanges.DERIBIT and position['contract_type'] == 'perpetual_future':
                        log(f"[{self}] 'size future' {size}")
                        log(f"[{self}] 'mark price' {mark_price}")
                        size = size / mark_price
                    if strategy.platform_id == Exchanges.DERIBIT and position['contract_type'] == 'spot':
                        deribit_balances = strategy.get_api_connector().get_balance()
                        if symbol != 'USDC' and symbol != 'SOL':
                            future_position = strategy.get_api_connector().get_position(symbol+'_PERP_USD')
                            size -= float(deribit_balances[symbol]['total_pl']) - float(future_position['total_pl'])
                        else:
                            size -= float(deribit_balances[symbol]['total_pl'])
                        log(f"[{self}] 'position size' {position['size']}")
                        log(f"[{self}] 'size' {size}")
                        log(f"[{self}] 'deribit_balances' {deribit_balances[symbol]}")

                        position['size'] = deribit_balances[symbol]["balance"]

                    position['delta'] =  size
                    position['gamma'] =  0
                    position['vega'] =  0
                    position['theta'] =  0

                # Compose user positions delta
                am_positions[strategy.id][symbol] = position["size"]
                am_positions[strategy.id][symbol] = {
                    "delta": round(float(position['delta'])*mark_price, 1),
                    "theta": round(float(position['theta']), 1),
                    "gamma": round(float(position['gamma']), 1),
                    "vega": round(float(position['vega']), 1),
                }

                base_asset_symbol = position['base_asset_symbol']
                # Compose user delta for current platform
                if not base_asset_symbol in am_global_delta[strategy.id]:
                    am_global_delta[strategy.id][base_asset_symbol] = {
                        "delta": float(position['delta'])*mark_price,
                        "theta": float(position['theta'])*mark_price,
                        "gamma": float(position['gamma'])*mark_price,
                        "vega": float(position['vega'])*mark_price,
                    }
                else:
                    am_global_delta[strategy.id][base_asset_symbol]['delta'] += float(position['delta'])*mark_price
                    am_global_delta[strategy.id][base_asset_symbol]['theta'] += float(position['theta'])*mark_price
                    am_global_delta[strategy.id][base_asset_symbol]['gamma'] += float(position['gamma'])*mark_price
                    am_global_delta[strategy.id][base_asset_symbol]['vega'] += float(position['vega'])*mark_price


                # Compose user delta for all platforms
                if not base_asset_symbol in am_global_delta[0]:
                    am_global_delta[0][base_asset_symbol] = {
                        "delta": float(position['delta'])*mark_price,
                        "theta": float(position['theta'])*mark_price,
                        "gamma": float(position['gamma'])*mark_price,
                        "vega": float(position['vega'])*mark_price,
                    }
                else:
                    am_global_delta[0][base_asset_symbol]["delta"] += float(position['delta'])*mark_price
                    am_global_delta[0][base_asset_symbol]["theta"] += float(position['theta'])*mark_price
                    am_global_delta[0][base_asset_symbol]["gamma"] += float(position['gamma'])*mark_price
                    am_global_delta[0][base_asset_symbol]["vega"] += float(position['vega'])*mark_price

        return am_global_delta, am_positions, am_positions_delta

    def _initialize_strategies(self):
        asset_manager_strategies = self.database.getTable("strategies", arg=f"WHERE asset_manager_id = {self.id}")

        strategies = []
        for index, am_strategy in asset_manager_strategies.iterrows():
            strategy = Strategy(am_strategy.id, self.database, self)
            if strategy.is_master_strat() and len(strategy.get_substrats()) > 0 and not strategy.is_disabled():
                strategies.append(strategy)
            elif am_strategy.type == "cefi" and am_strategy.api_key and am_strategy.private_key:
                if not strategy.is_disabled():
                    strategies.append(strategy)
            elif am_strategy.type == "defi" and am_strategy.wallet:
                if not strategy.is_disabled():
                    strategies.append(strategy)

        return strategies

    def __str__(self):
        return f"{self.id}-{self.name}"

class StrategyType(IntEnum):
    CEFI = 1
    DEFI = 2

class Strategy:

    def __init__(self, strategy_id, database, asset_manager=None, user_id=None):
        self.id = strategy_id
        self.database = database
        self.asset_manager = asset_manager
        self.user_id = user_id

        strategy_df = self.database.getTable("strategies", arg=f"WHERE id = {self.id}")
        self.feeder_strategy_shares_df = self.database.getTable("feeder_strategy_shares", arg=f"WHERE feeder_strategy_id = {self.id}")

        self.base_currency = strategy_df["base_currency"].values[0]
        self.email = strategy_df["tilvest_email"].values[0]
        self.code = strategy_df["code"].values[0]
        self.disabled = True if strategy_df["disabled"].values[0] == 1 else False
        self.master_strat = True if strategy_df["is_master_strat"].values[0] == 1 else False
        self.is_fees_payment = True if strategy_df["is_fees_payment"].values[0] == 1 else False
        self.is_margin = True if strategy_df["is_margin"].values[0] == 1 else False
        self.ramp_up = True if strategy_df["is_ramp_up"].values[0] == 1 else False
        self.is_flexible_earn = True if strategy_df["is_flexible_earn"].values[0] == 1 else False
        self.master_strat_id = int(strategy_df["master_strat_id"].values[0]) if strategy_df["master_strat_id"].values[0] else None
        self.substrat = True if strategy_df["master_strat_id"].values[0] != None else False
        self.fireblocks_account = strategy_df["fireblocks_account_name"].values[0]
        self.binance_link_subaccount = strategy_df["binance_link_subaccount_id"].values[0]
        self.feeder_investable = True if strategy_df["is_feeder_investable"].values[0] == 1 else False
        self.has_brokering_fees = True if strategy_df["has_brokering_fees"].values[0] == 1 else False
        self.market_brokering_fees = float(strategy_df["market_brokering_fees"].values[0])/100
        self.other_brokering_fees = float(strategy_df["other_brokering_fees"].values[0])/100

        self.mev_connector = MevCapital(connectors_config.get("MEV_CAPITAL_API_KEY"))

        self.binance_brokerage_strategy_id = 47
        self.binance_brokerage_master_account_id = "507323729"
        self.is_binance_master_account = True if self.id == self.binance_brokerage_master_account_id and self.binance_link_subaccount == self.binance_brokerage_master_account_id else False

        if not self.asset_manager:
            asset_manager_id = strategy_df["asset_manager_id"].values[0]
            self.asset_manager = AssetManager(asset_manager_id, self.database)

        self.substrats = []
        if self.master_strat:
            substrats_df = self.database.getTable("strategies", arg=f"WHERE master_strat_id = {self.id} AND disabled = 0")
            for index, substrat in substrats_df.iterrows():
                substrat = Strategy(substrat.id, self.database, self.asset_manager, user_id)
                self.substrats.append(substrat)

        self.name = strategy_df["name"].values[0]

        self.type = strategy_df["type"].values[0]
        self.benchmark = strategy_df["benchmark"].values[0]
        self.benchmark_composition = strategy_df["benchmark_composition"].values[0]
        self.benchmark_yield = strategy_df["benchmark_yield"].values[0]
        self.wallet_address = strategy_df["wallet"].values[0]
        self.wallet_sas_address = strategy_df["wallet_sas_address"].values[0]

        self.platform_id = int(strategy_df["platform_id"].values[0]) if strategy_df["platform_id"].values[0] else None
        self.exchange = Exchange(self.platform_id) if self.platform_id else None
        self.blockchain = strategy_df["blockchain"].values[0]

        self.api_key = strategy_df["api_key"].values[0]
        self.api_secret = strategy_df["private_key"].values[0]
        self.passphrase = strategy_df["passphrase"].values[0]
        self.testnet = True if strategy_df["use_testnet"].values[0] == 1 else False

        self.waiting_strategy = True if strategy_df["has_waiting_strategy"].values[0] == 1 else False
        self.waiting_strategy_id  = strategy_df["waiting_strategy_id"].values[0]
        self.is_deposit_withdraw = strategy_df["is_deposit_withdraw"].values[0]

        self.management_fees = strategy_df["management_fees"].values[0]
        self.performance_fees = strategy_df["performance_fees"].values[0]
        self.custodian_fees = strategy_df["custody_fees"].values[0]
        self.pricing_frequency = strategy_df["pricing_frequency"].values[0]
        self.entry_fees_am = strategy_df['entry_fees_am'].values[0]

        self.etherscan_api_key = connectors_config.get("ETHERSCAN_API_KEY")
        self.coinmarketcap_api_key = connectors_config.get("COINMARKETCAP_API_KEY")

        self.main_fireblocks_account = "[045]_TILVEST_MAIN_FIREBLOCKS"

        feeder_strategy_shares_df = self.database.getTable("feeder_strategy_shares ", arg=f"WHERE feeder_strategy_id = {self.id}")
        self.__is_feeder = True if not feeder_strategy_shares_df.empty else False
        self.feeder_investments = pd.DataFrame()
        if self.__is_feeder:
            for invested_strategy_id in feeder_strategy_shares_df["invested_strategy_id"].unique():
                self.feeder_investments = pd.concat([self.feeder_investments, pd.DataFrame([{
                'strategy': Strategy(invested_strategy_id, self.database, self.asset_manager),
                'invested_strategy_id': invested_strategy_id,
                'number_shares': feeder_strategy_shares_df[feeder_strategy_shares_df["invested_strategy_id"] == invested_strategy_id]["number_shares"].values[-1],
            }])])

        self.strategy_balances_df = self.database.getTable("users_balances", arg=f"WHERE strategy_id = {self.id}")
        if self.strategy_balances_df.empty:
            self.nav = 1
        else:
            self.nav = self.strategy_balances_df["nav"].values[-1]

        self.connector = None

        self.balances = {}
        self.profit = {}

    def get_type(self):
        return self.type

    def is_disabled(self):
        return self.disabled

    def is_master_strat(self):
        return self.master_strat

    def get_master_strat_id(self):
        return self.master_strat_id

    def is_ramp_up(self):
        return self.ramp_up

    def is_feeder_investable(self):
        return self.feeder_investable

    def is_payment_fees(self):
        return self.is_fees_payment

    def has_waiting_strategy(self):
        return self.waiting_strategy

    def is_substrat(self):
        return self.substrat

    def is_fees_wallet(self):
        return self.is_fees_wallet

    def is_feeder(self):
        return self.__is_feeder

    def get_substrats(self):
        return self.substrats

    def get_feeders(self):
        return self.feeder_investments

    def get_fireblocks_account(self):
        return self.fireblocks_account

    def get_binance_link_subaccount(self):
        return self.binance_link_subaccount

    def get_nav(self):
        return self.nav

    def get_api_connector(self):

        network = "testnet" if self.testnet else "mainnet"
        # if not self.is_api_platform_configured(platform_id):
        #     raise InvalidApiKeyError(f"API platform {platform_id} not configured for user {self}")

        if not self.connector:
            self.connector = self.__create_connector()

        return self.connector

    def check_ip_authorization(self):
        return self.get_api_connector().is_ip_authorized()

    def __create_connector(self):
        if self.type == 'cefi' or (self.is_master_strat() and len(self.substrats) > 0):
            if self.exchange == Exchanges.BINANCE and self.binance_link_subaccount and self.has_brokering_fees and self.id != self.binance_brokerage_strategy_id:
                brokerage_strategy = Strategy(self.binance_brokerage_strategy_id, self.database)
            else:
                brokerage_strategy = None
            return StrategyConnector(self, base_currency=self.base_currency, brokerage_strategy=brokerage_strategy)
        else:
            return DefiConnector(self, base_currency=self.base_currency)

    def is_api_platform_configured(self):
        network = "testnet" if self.testnet else "mainnet"

        if self.type == 'defi':
            return True
        if (self.api_key and self.api_secret and
            Exchange(self.platform_id).connector is not None):
            return True
        return False

    def get_wallet_address(self, currency=None):
        address = None
        if self.platform_id == Exchanges.DELTA_EXCHANGE or self.platform_id == Exchanges.DELTA_EXCHANGE_MARKET_MAKER:
            user_apis = self.database.getTable("crypto_users_apis", arg=f"WHERE user_id = {self.id} AND platform_id = {self.platform_id}")
            if not user_apis.empty:
                address = {"address": user_apis["wallet_address"].values[0]}
        else:
            if not (address := self.get_api_connector().get_wallet_address(currency)):
                address = self.get_api_connector().create_wallet_address(currency)
        return address

    def log_trade(self, order_id, symbol, size, side, order_type, status, execution_price):
        now = datetime.datetime.now()
        trade_values = pd.DataFrame()
        caller = sys.argv[0]

        existing_order = self.database.getTable("log_user_trades", arg=f"WHERE order_id = '{order_id}' AND strategy_id = {self.id}")
        if existing_order.empty:
            trade_values = pd.concat([trade_values, pd.DataFrame([{
                'strategy_id': self.id,
                'order_id': order_id,
                'status': status,
                'symbol': symbol,
                'size': size,
                'side': side,
                'execution_price': execution_price,
                'order_type': order_type,
                'platform_id': self.platform_id,
                'user_id': self.user_id,
                'caller': caller,
                'created_at': now,
                'updated_at': now,
            }])])
            self.database.append_to_table("log_user_trades", trade_values)
        else:
            self.database.execute(f"UPDATE log_user_trades SET status = %s, execution_price = %s, updated_at = %s WHERE order_id = '{order_id}' AND strategy_id = {self.id}",
                params=(status, execution_price, now))

    def get_positions(self):
        positions = {}
        if self.is_master_strat and len(self.substrats) > 0:
            for substrat in self.substrats:
                substrat_positions = substrat.get_positions()
                for symbol, position in substrat_positions.items():
                    if symbol not in positions:
                        positions[symbol] = position
                    else:
                        positions[symbol]["entry_price"] = (positions[symbol]["entry_price"]*positions[symbol]["size"]+position["entry_price"]*position["size"])/(positions[symbol]["size"]+position["size"])
                        positions[symbol]["size"] += position["size"]
                        positions[symbol]["base_currency_amount"] += position["base_currency_amount"]
        else:
            positions = self.get_api_connector().get_positions()
        return positions

    def update_buy_crypto(self, fake_run=False):
        log(f"[{self}] Update deposit/withdraw history")

        # Loop substrats when master strat with substrats
        if self.is_master_strat() and len(self.substrats) > 0:
            for substrat in self.substrats:
                substrat.update_buy_crypto(fake_run=fake_run)
            return

        # Filter existing history entries for user
        strategy_history = self.database.getTable("buy_crypto_history", arg=f"WHERE strategy_id = {self.id}")

        connector = self.get_api_connector()

        # Retrieve user deposit/withdraw history since beginning of the year
        if strategy_history.empty:
            operations = connector.get_deposit_withdraw_history(reload=True)
        # Retrieve user deposit/withdraw history since last database entry
        else:
            most_recent_date = strategy_history["date"].max()
            operations = connector.get_deposit_withdraw_history(most_recent_date, datetime.datetime.now(), reload=True)

        # Compose operation entries
        operation_history = pd.DataFrame()
        for operation in operations:

            if strategy_history[strategy_history["order_number"] == operation["operation_id"]].empty:

                log(f"- New transaction '{operation['operation_id']}' : {operation['type']} of {operation['destination_amount']} {operation['destination_symbol']}")
                now = datetime.datetime.now()
                operation_history = pd.concat([operation_history, pd.DataFrame([{
                    "strategy_id": self.id,
                    "transaction_type": operation["type"],
                    "date": operation["date"],
                    "source_symbol": operation["source_symbol"],
                    "source_amount": operation["source_amount"],
                    "destination_symbol": operation["destination_symbol"],
                    "destination_amount": operation["destination_amount"],
                    "base_currency_symbol": operation["base_currency_symbol"],
                    "base_currency_amount": operation["base_currency_amount"],
                    "fees": operation["fees"],
                    "order_number": operation["operation_id"],
                    "wallet_address": operation["wallet_address"],
                    "created_at": now,
                    "updated_at": now,
                }])])

        # Append operations to database
        if not fake_run and not operation_history.empty:
            operation_history.to_sql('buy_crypto_history', con = self.database.engine, if_exists = 'append', index=False)

    def get_active_orders(self, symbol):
        return self.get_api_connector().get_active_orders(symbol)

    def get_order(self, symbol, order_id):
        return self.get_api_connector().get_order_state(symbol, order_id)

    def get_transaction_history(self):
        return self.get_api_connector().get_transaction_history()

    def get_balance(self, reload=False):
        self.__update_balances(reload)
        return self.balances

    def get_total_balance(self):
        balances = self.get_api_connector().get_balance(as_base_currency=True)
        total_balance = sum(balance["balance"] for balance in balances.values())

        for index, feeder_investment in self.feeder_investments.iterrows():
            strategy = feeder_investment["strategy"]
            nav = strategy.get_nav()
            rate = CbForex(self.database).get_rate(self.base_currency, strategy.base_currency)
            shares = feeder_investment["number_shares"]
            total_balance += nav/rate*shares

        return total_balance

    def __update_balances(self, reload=False):
        if self.balances and not reload:
            return self.balances
        else:

            # Handle connectors not implemented yet
            connector = self.get_api_connector()
            if not connector:
                self.asset_manager.balances[self.id] = {
                    "balance": 0,
                    "available_balance": 0,
                    "max_investment": 0
                }
                return

            extracted_balances = self.get_api_connector().get_balance(as_base_currency=True)
            balance = 0
            available_balance = 0

            for symbol, bal in extracted_balances.items():
                balance += float(bal["balance"])
                available_balance += float(bal["available_balance"])

            self.balances["balance"] = balance
            self.balances["available_balance"] = available_balance

            # Add feeder investments
            for index, feeder_investment in self.feeder_investments.iterrows():
                strategy = feeder_investment["strategy"]
                nav = strategy.get_nav()
                rate = CbForex(self.database).get_rate(self.base_currency, strategy.base_currency)
                shares = feeder_investment["number_shares"]
                self.balances["balance"] += nav/rate*shares

            # Save at all time 30% of the portfolio for margin call
            #check that the user has 20 / 80 %
            minimum_balance_option = 0.3 * balance
            client_investment = available_balance - minimum_balance_option

            # No strategy should be more than 22% of the total portfolio of the user
            if client_investment > 1000:
                client_investment = min(0.22*balance, client_investment)
            else:
                client_investment = client_investment*0.8

            self.balances["max_investment"] = float(client_investment)
            return self.balances

    def update_balances(self, strategy, user_balances, thread_name, fake_run, nav, hwm, number_shares, is_update_shares):
        # Update master strat with substrat
        if strategy.is_master_strat() and len(strategy.get_substrats()) > 0:

            master_strat_total_balance = 0
            master_strat_total_deposit = 0
            master_strat_total_withdraw = 0
            master_strat_total_profit = 0

            for substrat in strategy.get_substrats():

                log(f"{thread_name} - Update balances for strategy : {substrat}")

                # Get strategy balances
                profit = substrat.get_profit()

                self.update_strategy_balances(strategy, user_balances, profit["total_balance"], profit["total_deposit"], profit["total_withdraw"], profit["total_profit"], thread_name, fake_run, nav, hwm, number_shares, is_update_shares)

                master_strat_total_balance += profit["total_balance"]
                master_strat_total_deposit += profit["total_deposit"]
                master_strat_total_withdraw += profit["total_withdraw"]
                master_strat_total_profit += profit["total_profit"]

            log(f"{thread_name} - Update balances for strategy : {strategy}")

            # Add strategy hedging values
            hedgings = self.database.getTable("strategy_expired_hedging ", arg=f"WHERE strategy_id = {strategy.id}")
            if not hedgings.empty:
                master_strat_total_balance += self.mev_connector.get_hedging_sum()

            feeder_strategy_shares = self.database.getTable("feeder_strategy_shares ", arg=f"WHERE feeder_strategy_id = {strategy.id}")
            if len(feeder_strategy_shares) > 0 :
                for invested_strategy_id in feeder_strategy_shares.invested_strategy_id.unique():
                    print('invested_strategy_id',invested_strategy_id)
                    #check if need update recent
                    invested_strategy = Strategy(invested_strategy_id, self.database)
                    user_balances_feeder = self.database.getTable("users_balances" , arg=f"WHERE strategy_id = {invested_strategy_id}" )
                    strat_nav = float(user_balances_feeder[user_balances_feeder["strategy_id"] == invested_strategy_id]['nav'].values[-1])/CbForex(self.database).get_rate(strategy.base_currency, invested_strategy.base_currency)

                    master_strat_total_balance += strat_nav*feeder_strategy_shares[feeder_strategy_shares['invested_strategy_id'] == invested_strategy_id]['number_shares'].values[-1]

                master_strat_total_profit = master_strat_total_balance - master_strat_total_deposit + master_strat_total_withdraw

            print('master_strat_total_balance',master_strat_total_balance)
            self.update_strategy_balances(strategy, user_balances, master_strat_total_balance, master_strat_total_deposit, master_strat_total_withdraw, master_strat_total_profit, thread_name, fake_run, nav, hwm, number_shares, is_update_shares)

        # Update master strat without substrat
        else:
            log(f"{thread_name} - Update balances for strategy : {strategy}")

            # Get strategy balances
            profit = strategy.get_profit()

            self.update_strategy_balances(strategy, user_balances, profit["total_balance"], profit["total_deposit"], profit["total_withdraw"], profit["total_profit"], thread_name, fake_run, nav, hwm, number_shares, is_update_shares)

    def update_strategy_balances(self, strategy, user_balances, total_balance, total_deposit, total_withdraw, total_profit, thread_name, fake_run, nav, last_hwm, number_shares, is_update_shares):

        total_balance = round(total_balance, 2)
        total_deposit = round(total_deposit, 2)
        total_withdraw = round(total_withdraw, 2)
        total_profit = round(total_profit, 2)

        if len(user_balances[user_balances["strategy_id"] == strategy.id]) > 0:
            if not is_update_shares:
                number_shares = round(float(user_balances[user_balances["strategy_id"] == strategy.id]['number_shares'].values[-1]), 2)
                if number_shares > 0:
                    last_hwm = round(float(user_balances[user_balances["strategy_id"] == strategy.id]['last_hwm'].values[-1]), 2)
                    nav = round(float(total_balance / number_shares), 7)
                # if nav > float(last_hwm):
                #     last_hwm = nav
                else:
                    nav = 1
                    last_hwm = round(float(user_balances[user_balances["strategy_id"] == strategy.id]['last_hwm'].values[-1]), 2)
        else:
            #firt valuation, nav price is 1
            if not is_update_shares:
                number_shares = round(total_balance, 2)
                nav = 1
                last_hwm = nav

        if len(user_balances[user_balances["strategy_id"] == strategy.id]) == 0:
            #first time valoriastion
            nav_benchmark = nav
            last_hwm_benchmark = nav
        else:
            nav_benchmark = float(user_balances[user_balances["strategy_id"] == strategy.id]['nav_benchmark'].values[-1])
            last_hwm_benchmark = float(user_balances[user_balances["strategy_id"] == strategy.id]['last_hwm_benchmark'].values[-1])

        base_currency_rate = CbForex(self.database).get_rate("EUR", strategy.base_currency)

        if strategy.benchmark and len(strategy.benchmark) > 0:
            print(strategy.benchmark)
            if strategy.benchmark == "USD" or strategy.benchmark == "EUR":
                computed_symbol_mark_price  = 1
            else:
                if "//" in strategy.benchmark:
                    list_bench = strategy.benchmark.split('//')
                    computed_symbol_mark_price = 0
                    for composition in list_bench:
                        weight = float(composition.split('_')[0])/100
                        symbol = composition.split('_')[1]
                        symbol = symbol.split(' ')[0]

                        symbol_mark_price = CbForex(self.database).get_rate(symbol, 'USD')*base_currency_rate
                        computed_symbol_mark_price += weight*symbol_mark_price

                else:
                    weight = float(strategy.benchmark.split('_')[0])/100
                    symbol = strategy.benchmark.split('_')[1]

                    symbol_mark_price = CbForex(self.database).get_rate(symbol, 'USD')
                    computed_symbol_mark_price = symbol_mark_price*base_currency_rate

            if len(user_balances[user_balances["strategy_id"] == strategy.id]) == 0:
                last_benchmark_price = 1
            else:
                last_benchmark_price = float(user_balances[user_balances["strategy_id"] == strategy.id]['last_benchmark_price'].values[-1])

            if last_benchmark_price == 0:
                nav_benchmark = 1
            else:
                if nav_benchmark == 0:
                    nav_benchmark = 1
                nav_benchmark = (nav_benchmark*(computed_symbol_mark_price / last_benchmark_price))

            print('nav_benchmark',nav_benchmark)
            print('symbol_mark_price',computed_symbol_mark_price)
            print('last_benchmark_price',last_benchmark_price)
            print('base_currency_rate',base_currency_rate)

            last_benchmark_price = computed_symbol_mark_price
        else:
            nav_benchmark = nav_benchmark*(1+ (strategy.benchmark_yield*(0.25/365)))
            last_benchmark_price = nav_benchmark

        if  nav_benchmark >  last_hwm_benchmark:
            last_hwm_benchmark = nav_benchmark

        # Print balances
        log(f"{thread_name} - [{strategy}]")
        log(f"{thread_name} -    Total Balance : {total_balance}")
        log(f"{thread_name} -    Total deposit : {total_deposit}")
        log(f"{thread_name} -    Total withdraw : {total_withdraw}")
        log(f"{thread_name} -    Total profit : {total_profit}")
        log(f"{thread_name} -    Total shares : {number_shares}")
        log(f"{thread_name} -    Nav benchmark : {nav_benchmark}")
        log(f"{thread_name} -    last_hwm_benchmark : {last_hwm_benchmark}")
        log(f"{thread_name} -    last_benchmark_price : {last_benchmark_price}")
        log(f"{thread_name} -    nav price : {nav}")
        log(f"{thread_name} -    Last hwm : {last_hwm}")

        if not fake_run:

            now = datetime.datetime.now()
            today = datetime.date.today()

            # Retrieve existing entry
            balances = self.database.getTable("users_balances", arg=f"WHERE strategy_id = {strategy.id}")
            balances_history = self.database.getTable("users_balances_history", arg=f"WHERE strategy_id = {strategy.id} AND date = '{today}'")

            if balances.empty:
                # Create new entry
                self.database.execute("""INSERT INTO users_balances (strategy_id, total_balance, total_deposit, total_withdrawal, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price,created_at, updated_at)
                                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    params=(strategy.id, total_balance, total_deposit, total_withdraw, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, now, now))
            else:
                # Update existing entry
                self.database.execute("""UPDATE users_balances SET total_balance = %s, total_deposit = %s, total_withdrawal =%s, total_profit = %s, number_shares = %s, nav = %s, last_hwm = %s, nav_benchmark = %s, last_hwm_benchmark = %s, last_benchmark_price = %s, updated_at = %s
                                            WHERE strategy_id = %s
                                    """,
                                    params=(total_balance, total_deposit, total_withdraw, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, now, float(strategy.id)))

            if balances_history.empty:
                # Create new entry
                self.database.execute("""INSERT INTO users_balances_history (strategy_id, total_balance, total_deposit, total_withdrawal, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, date, created_at, updated_at)
                                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    """,
                                    params=(strategy.id, total_balance, total_deposit, total_withdraw, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, today, now, now))
            else:
                # Update existing entry
                self.database.execute("""UPDATE users_balances_history SET total_balance = %s, total_deposit = %s, total_withdrawal = %s, total_profit = %s, number_shares = %s, nav = %s, last_hwm = %s, nav_benchmark = %s, last_hwm_benchmark = %s, last_benchmark_price = %s, updated_at = %s
                                            WHERE strategy_id = %s AND date = %s
                                    """,
                                    params=(total_balance, total_deposit, total_withdraw, total_profit, number_shares, nav, last_hwm, nav_benchmark, last_hwm_benchmark, last_benchmark_price, now, float(strategy.id), today))


    def get_profit(self, reload=False):
        #print(f"IN {type(self)} -> get_profit")
        self.__update_profit(reload)
        return self.profit

    def __update_profit(self, reload=False):
        #print(f"IN {type(self)} -> __update_profit")
        if self.profit and not reload:
            return self.profit
        else:
            self.profit = self.get_api_connector().get_profit()

            if self.asset_manager.id == 17 and not self.is_master_strat() and self.type == 'defi':
                self.mev_connector = MevCapital(connectors_config.get("MEV_CAPITAL_API_KEY"))
                self.profit["total_balance"] = float(self.mev_connector.get_balance(self.wallet_address)['USDC']['balance'])


            # Add strategy hedging values
            hedgings = self.database.getTable("strategy_expired_hedging ", arg=f"WHERE strategy_id = {self.id}")
            if not hedgings.empty:
                try:
                    self.mev_connector = MevCapital(connectors_config.get("MEV_CAPITAL_API_KEY"))
                    self.profit["total_balance"] += self.mev_connector.get_hedging_sum()
                except:
                    pass

                #self.profit["total_balance"] += hedgings["mtm_value"].sum()
                #print(f"### ADD {hedgings['mtm_value'].sum()}")

            """
            # Add invested strategies hedging values
            for feeder_investment in self.feeder_investments:
                hedgings = self.database.getTable("strategy_expired_hedging ", arg=f"WHERE strategy_id = {feeder_investment['strategy'].id}")
                if not hedgings.empty:
                    self.profit["total_balance"] += hedgings["mtm_value"].sum()
                    print(f"### ADD {hedgings['mtm_value'].sum()}")
            """

            # Add feeder investments
            for index, feeder_investment in self.feeder_investments.iterrows():
                strategy = feeder_investment["strategy"]
                nav = strategy.get_nav()
                rate = CbForex(self.database).get_rate(self.base_currency, strategy.base_currency)
                shares = feeder_investment["number_shares"]
                #print(f"--- Add feeder investment to total_balance : {nav/rate*shares}")
                self.profit["total_balance"] += nav/rate*shares

            # Recompute profit with feeder investments
            #print(f"--- Recompute profit with new total_balance")
            self.profit["total_profit"] = self.profit["total_balance"] - self.profit["total_deposit"] + self.profit["total_withdraw"]

            return self.profit

    def transfer_to_strategy(self, fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=None, fake_run=False):
        if self == strategy:
            return []
        log(f"[{self}] Create transfer of {amount} {symbol} from '{self}' to strategy {strategy}")

        source_type = self.type
        strategy_source_id = self.id
        destination_type = strategy.get_type()
        strategy_destination_id = strategy.id

        # Uses substrat when master strat doesn't have a dedicated account
        if self.is_master_strat() and len(self.substrats) > 0:
            for substrat in self.substrats:
                if self.fireblocks_account == substrat.get_fireblocks_account():
                    source_type = substrat.get_type()
                    strategy_source_id = substrat.id
                    break
        if strategy.is_master_strat() and len(strategy.substrats) > 0:
            for substrat in strategy.substrats:
                if strategy.get_fireblocks_account() == substrat.get_fireblocks_account():
                    destination_type = substrat.get_type()
                    strategy_destination_id = substrat.id
                    break

        # Both strategies are cefi
        if source_type == destination_type == "cefi":
            transactions = self.__transfer_cefi_to_cefi(fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=transaction_type, fake_run=fake_run)
        # Both strategies are defi
        elif source_type == destination_type == "defi":
            transactions = self.__transfer_defi_to_defi(fireblocks_connector, symbol, amount, strategy, transaction_type=transaction_type, fake_run=fake_run)
        # Transfer from cefi to defi
        elif source_type == "cefi":
            transactions = self.__transfer_cefi_to_defi(fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=transaction_type, fake_run=fake_run)
        # Transfer from defi to cefi
        else:
            transactions = self.__transfer_defi_to_cefi(fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=transaction_type, fake_run=fake_run)

        try:
            if not fake_run:
                if all(transaction["status"]=="COMPLETED" for transaction in transactions):
                    if transaction_type == "brokering_fees":
                        src_id = transactions[0]["id"] if "id" in transactions[0] else None
                        now = datetime.datetime.now()
                        self.database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            params=(strategy_source_id, 'brokering_fees', src_id, now, amount, symbol, 0, 0, 1, True, now, now))
                    else:
                        src_id = transactions[0]["id"] if "id" in transactions[0] else None
                        dest_id = transactions[-1]["id"] if "id" in transactions[-1] else None
                        now = datetime.datetime.now()
                        self.database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            params=(strategy_source_id, 'withdraw', src_id, now, amount, symbol, 0, 0, 1, True, now, now))
                        self.database.execute("""INSERT INTO strategy_cashflows (strategy_id, transaction_type, last_tx_id, timestamp, amount, currency, number_shares_before, number_shares_after, nav, tilvest_approved, created_at, updated_at)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            params=(strategy_destination_id, 'deposit', dest_id, now, amount, symbol, 0, 0, 1, True, now, now))
        except Exception as e:
            log(f"[{self}] Exception occured while logging transfer : {type(e)} - {e}")
            log(f"{format_traceback(e.__traceback__)}")

        return transactions

    def __transfer_cefi_to_cefi(self, fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=None, fake_run=False):
        log(f"[{self}] Transfer Cefi to Cefi")

        transactions = []
        source_type = self.type
        source_exchange = self.exchange
        destination_type = strategy.get_type()
        destination_exchange = strategy.exchange

        # Uses substrat when master strat doesn't have a dedicated account
        if self.is_master_strat() and len(self.substrats) > 0:
            for substrat in self.substrats:
                if self.fireblocks_account == substrat.get_fireblocks_account():
                    source_type = substrat.get_type()
                    source_exchange = substrat.exchange
                    break
        if strategy.is_master_strat() and len(strategy.substrats) > 0:
            for substrat in strategy.substrats:
                if strategy.get_fireblocks_account() == substrat.get_fireblocks_account():
                    destination_type = substrat.get_type()
                    destination_exchange = substrat.exchange
                    break

        # Check transfer capability
        if source_exchange.platform_id not in [Exchanges.BINANCE, Exchanges.COINBASE, Exchanges.DERIBIT, Exchanges.KRAKEN]:
            log(f"[{self}][ERROR] Can't withdraw from exchange '{source_exchange}'")
            return transactions

        source_exchange_master_account = source_exchange.get_fireblocks_account(self.database)
        destination_exchange_master_account = destination_exchange.get_fireblocks_account(self.database)

        # Round size depending on exchange and asset
        if source_exchange == Exchanges.DERIBIT or destination_exchange == Exchanges.DERIBIT:
            amount = self.__transfer_round_size(Exchanges.DERIBIT, symbol, amount)

        # Strategies on different exchanges
        if source_exchange != destination_exchange:

            # From source subaccount to source master account
            if self.fireblocks_account != source_exchange_master_account and source_exchange != Exchanges.KRAKEN:
                # From binance subaccount
                if source_exchange == Exchanges.BINANCE:
                    if self.is_margin:
                        self.__binance_margin_to_spot_transfer(self, symbol, amount, fake_run=fake_run)
                    if self.is_flexible_earn:
                        self.__binance_redeem(self, symbol, amount, fake_run=fake_run)
                    transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, self.get_binance_link_subaccount(), None, transaction_type=transaction_type, fake_run=fake_run)
                # From exchange subaccount
                else:
                    transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", self.fireblocks_account, "EXCHANGE_ACCOUNT", source_exchange_master_account, transaction_type=transaction_type, fake_run=fake_run)

                transactions.append(transaction)
                if transaction["status"] != "COMPLETED":
                    return transactions

            # Directly send from source master account to destination subaccount
            if source_exchange == Exchanges.COINBASE and destination_exchange == Exchanges.BINANCE:
                # Set confirmation threshold depending on destination exchange
                confirmation_threshold = 64 if destination_exchange == Exchanges.BINANCE else None
                transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_exchange_master_account, "INTERNAL_WALLET", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)
                transactions.append(transaction)
                if strategy.is_margin:
                    self.__binance_spot_to_margin_transfer(strategy, symbol, amount, fake_run=fake_run)
                return transactions

            # Directly send from source master account to destination subaccount
            elif destination_exchange == Exchanges.KRAKEN:
                transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_exchange_master_account, "INTERNAL_WALLET", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)
                transactions.append(transaction)
                return transactions

            #elif source_exchange == Exchanges.KRAKEN and destination_exchange == Exchanges.COINBASE:
            #    transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", self.fireblocks_account, "INTERNAL_WALLET", destination_exchange_master_account, transaction_type=transaction_type, fake_run=fake_run)
            #    #transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", self.fireblocks_account, "INTERNAL_WALLET", "[044]_TILVEST_MAIN_COINBASE_USDT_KRAKEN", transaction_type=transaction_type, fake_run=fake_run)
            #    transactions.append(transaction)
            #    return transactions

            # Send directly from source subaccount to destination master account, then to subaccount
            elif source_exchange == Exchanges.KRAKEN:
                transaction = self.__transfer_to_cefi_master_account(fireblocks_connector, source_exchange, destination_exchange, self.fireblocks_account, destination_exchange_master_account, symbol, amount, transaction_type=transaction_type, fake_run=fake_run)
                transactions.append(transaction)
                if transaction["status"] != "COMPLETED":
                    return transactions

            # Send from source master account to destination master account then to subaccount
            else:
                transaction = self.__transfer_to_cefi_master_account(fireblocks_connector, source_exchange, destination_exchange, source_exchange_master_account, destination_exchange_master_account, symbol, amount, transaction_type=transaction_type, fake_run=fake_run)
                transactions.append(transaction)
                if transaction["status"] != "COMPLETED":
                    return transactions

                #"""
                #TODO: Simulate transfer fees
                if source_exchange == Exchanges.BINANCE and destination_exchange == Exchanges.COINBASE:
                    if symbol == "USDT_ERC20":
                        amount -= 8
                    elif symbol == "USDC":
                        amount -= 10
                if source_exchange == Exchanges.DERIBIT and destination_exchange == Exchanges.COINBASE:
                    if symbol == "USDT_ERC20":
                        amount -= 3.4499
                    elif symbol == "USDC":
                        amount -= 10
                    elif symbol == "BTC":
                        amount -= 0.00032
                    elif symbol == "ETH":
                        amount -= 0.0009
                #"""

            # Send to destination subaccount
            if strategy.get_fireblocks_account() != destination_exchange_master_account:

                # To binance subaccount
                if destination_exchange == Exchanges.BINANCE:
                    transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, None, strategy.get_binance_link_subaccount(), transaction_type=transaction_type, fake_run=fake_run)
                    if strategy.is_margin:
                        self.__binance_spot_to_margin_transfer(strategy, symbol, amount, fake_run=fake_run)
                # To exchange subaccount
                else:
                    transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", destination_exchange_master_account, "EXCHANGE_ACCOUNT", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)

                transactions.append(transaction)
                if transaction["status"] != "COMPLETED":
                    return transactions

        # Both strategies on the same exchange
        else:

            # On Binance
            if source_exchange == Exchanges.BINANCE:

                # Master account to subaccount
                if self.fireblocks_account == source_exchange_master_account:
                    transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, None, strategy.get_binance_link_subaccount(), transaction_type=transaction_type, fake_run=fake_run)
                    if strategy.is_margin:
                        self.__binance_spot_to_margin_transfer(strategy, symbol, amount, fake_run=fake_run)
                # Subaccount to master account
                elif strategy.get_fireblocks_account() == destination_exchange_master_account:
                    if strategy.is_margin:
                        self.__binance_margin_to_spot_transfer(strategy, symbol, amount, fake_run=fake_run)
                    if self.is_flexible_earn:
                        self.__binance_redeem(self, symbol, amount, fake_run=fake_run)
                    transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, self.get_binance_link_subaccount(), None, transaction_type=transaction_type, fake_run=fake_run)
                # Subaccount to subaccount
                else:
                    if self.is_margin:
                        self.__binance_margin_to_spot_transfer(self, symbol, amount, fake_run=fake_run)
                    if self.is_flexible_earn:
                        self.__binance_redeem(self, symbol, amount, fake_run=fake_run)
                    transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, self.get_binance_link_subaccount(), strategy.get_binance_link_subaccount(), transaction_type=transaction_type, fake_run=fake_run)
                    if strategy.is_margin:
                        self.__binance_spot_to_margin_transfer(strategy, symbol, amount, fake_run=fake_run)

                transactions.append(transaction)
                if transaction["status"] != "COMPLETED":
                    return transactions

            # On another exchange
            else:

                # From subaccount
                if self.fireblocks_account != source_exchange_master_account:
                    transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", self.fireblocks_account, "EXCHANGE_ACCOUNT", source_exchange_master_account, transaction_type=transaction_type, fake_run=fake_run)
                    transactions.append(transaction)
                    if transaction["status"] != "COMPLETED":
                        return transactions

                # To subaccount
                if strategy.get_fireblocks_account() != destination_exchange_master_account:
                    transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", destination_exchange_master_account, "EXCHANGE_ACCOUNT", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)
                    transactions.append(transaction)
                    if transaction["status"] != "COMPLETED":
                        return transactions

        return transactions

    def __transfer_to_cefi_master_account(self, fireblocks_connector, source_exchange, destination_exchange, source_account, destination_account, symbol, amount, transaction_type=None, fake_run=False):
        transactions = []
        # Set confirmation threshold depending on destination exchange
        confirmation_threshold = 64 if destination_exchange == Exchanges.BINANCE else None

        # Transfer to destination exchange master account
        if source_exchange == Exchanges.BINANCE and destination_exchange == Exchanges.COINBASE and (symbol == "USDC" or symbol == "USDC_ARB_3SBJ"):
            confirmation_threshold = 30
            # Transfer on internal wallet for coinbase
            transaction = self.__make_fireblocks_transfer(fireblocks_connector, "USDC_ARB_3SBJ", amount, "EXCHANGE_ACCOUNT", source_account, "INTERNAL_WALLET", destination_account, transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)
            if not fake_run:
                log(f"[{self}] Waits 20mins for fund availability on Coinbase")
                #TODO: To replace with active waiting on funds availability
                time.sleep(20*60)
        elif destination_exchange == Exchanges.COINBASE:
            if symbol == "USDC":
                symbol = "USDC_ARB_3SBJ"
            confirmation_threshold = 30
            # Transfer on internal wallet for coinbase
            transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_account, "INTERNAL_WALLET", destination_account, transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)
        elif source_exchange == Exchanges.DERIBIT and destination_exchange == Exchanges.BINANCE:
            # Transfer on internal wallet for binance
            transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_account, "INTERNAL_WALLET", destination_account, transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)
        else:
            # Transfer on exchange wallet for other exchanges
            transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_account, "EXCHANGE_ACCOUNT", destination_account, transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)

        return transaction
        transactions.append(transaction)
        if transaction["status"] != "COMPLETED":
            return transactions

        #TODO: Simulate transfer fees
        if source_exchange == Exchanges.BINANCE and destination_exchange == Exchanges.COINBASE:
            if symbol == "USDT_ERC20":
                amount -= 8
            elif symbol == "USDC":
                amount -= 10
        if source_exchange == Exchanges.DERIBIT and destination_exchange == Exchanges.COINBASE:
            if symbol == "USDT_ERC20":
                amount -= 3.4499
            elif symbol == "USDC":
                amount -= 10
            elif symbol == "BTC":
                amount -= 0.00032
            elif symbol == "ETH":
                amount -= 0.0009

    def __transfer_defi_to_defi(self, fireblocks_connector, symbol, amount, strategy, transaction_type=None, fake_run=False):
        log(f"[{self}] Transfer Defi to Defi")

        transactions = []
        transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "VAULT_ACCOUNT", self.fireblocks_account, "VAULT_ACCOUNT", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)
        transactions.append(transaction)
        return transactions

    def __transfer_cefi_to_defi(self, fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=None, fake_run=False):
        log(f"[{self}] Transfer Cefi to Defi")

        transactions = []
        source_exchange = self.exchange

        # Check transfer capability
        if source_exchange.platform_id not in [Exchanges.BINANCE, Exchanges.COINBASE, Exchanges.DERIBIT, Exchanges.KRAKEN]:
            log(f"[{self}][ERROR] Can't withdraw from exchange '{source_exchange}'")
            return transactions

        # Uses substrat when master strat doesn't have a dedicated account
        if self.is_master_strat() and len(self.substrats) > 0:
            for substrat in self.substrats:
                if self.fireblocks_account == substrat.get_fireblocks_account():
                    source_exchange = substrat.exchange
                    break

        source_exchange_master_account = source_exchange.get_fireblocks_account(self.database)

        # Round size depending on exchange and asset
        if source_exchange == Exchanges.DERIBIT:
            amount = self.__transfer_round_size(Exchanges.DERIBIT, symbol, amount)

        # From subaccount
        if self.fireblocks_account != source_exchange_master_account:

            # From Binance
            if self.exchange == Exchanges.BINANCE:
                if self.is_margin:
                    self.__binance_margin_to_spot_transfer(self, symbol, amount, fake_run=fake_run)
                if self.is_flexible_earn:
                    self.__binance_redeem(self, symbol, amount, fake_run=fake_run)
                transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, self.get_binance_link_subaccount(), None, transaction_type=transaction_type, fake_run=fake_run)

            # From another exchange
            else:
                transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", self.fireblocks_account, "EXCHANGE_ACCOUNT", source_exchange_master_account, transaction_type=transaction_type, fake_run=fake_run)

            transactions.append(transaction)
            if transaction["status"] != "COMPLETED":
                return transactions

        # To defi account
        transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", source_exchange_master_account, "VAULT_ACCOUNT", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)
        transactions.append(transaction)

        return transactions

    def __transfer_defi_to_cefi(self, fireblocks_connector, binance_link_connector, symbol, amount, strategy, transaction_type=None, fake_run=False):
        log(f"[{self}] Transfer Defi to Cefi")

        transactions = []
        destination_exchange = strategy.exchange

        # Uses substrat when master strat doesn't have a dedicated account
        if strategy.is_master_strat() and len(strategy.substrats) > 0:
            for substrat in strategy.substrats:
                if strategy.get_fireblocks_account() == substrat.get_fireblocks_account():
                    destination_exchange = substrat.exchange
                    break

        destination_exchange_master_account = destination_exchange.get_fireblocks_account(self.database)

        # Round size depending on exchange and asset
        if destination_exchange == Exchanges.DERIBIT:
            amount = self.__transfer_round_size(Exchanges.DERIBIT, symbol, amount)

        # Set confirmation threshold depending on destination exchange
        confirmation_threshold = 64 if destination_exchange == Exchanges.BINANCE else None

        # Transfer to exchange master account
        transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "VAULT_ACCOUNT", self.fireblocks_account, "EXCHANGE_ACCOUNT", destination_exchange_master_account, transaction_type=transaction_type, fake_run=fake_run, confirmation_threshold=confirmation_threshold)
        transactions.append(transaction)

        # To subaccount
        if strategy.get_fireblocks_account() != destination_exchange_master_account:

            # To binance subaccount
            if destination_exchange == Exchanges.BINANCE:
                transaction = self.__make_binance_link_transfer(binance_link_connector, symbol, amount, None, strategy.get_binance_link_subaccount(), transaction_type=transaction_type, fake_run=fake_run)
                if strategy.is_margin:
                    self.__binance_spot_to_margin_transfer(strategy, symbol, amount, fake_run=fake_run)
            # To exchange subaccount
            else:
                transaction = self.__make_fireblocks_transfer(fireblocks_connector, symbol, amount, "EXCHANGE_ACCOUNT", destination_exchange_master_account, "EXCHANGE_ACCOUNT", strategy.get_fireblocks_account(), transaction_type=transaction_type, fake_run=fake_run)

            transactions.append(transaction)

        return transactions

    def __make_fireblocks_transfer(self, fireblocks_connector, symbol, amount, src_type, src_name, dest_type, dest_name, database_update=True, wait=True, timeout=30, transaction_type=None, fake_run=False, confirmation_threshold=None):
        log(f"[{self}] Create transfer of '{amount}' {symbol} from {src_type} '{src_name}' to {dest_type} '{dest_name}'")

        if not fake_run:
            transaction = fireblocks_connector.make_transfer(symbol, amount, src_type, src_name, dest_type, dest_name)
            log(f"[{self}] {transaction}")

            if confirmation_threshold:
                fireblocks_connector.set_confirmation_threshold(transaction["id"], confirmation_threshold)

            if database_update and transaction_type:
                FireblocksTransfer(transaction, transaction_type).to_database(self.database)

            if wait:
                status = ""
                transaction_id = transaction["id"]
                while status not in ["FAILED", "BLOCKED", "COMPLETED", "CANCELLED"]:
                    log(f"[{self}] Check transfer '{transaction_id}' status")
                    transaction = fireblocks_connector.get_transaction(transaction_id)
                    status = transaction["status"]
                    log(f"[{self}] Transfer '{transaction_id}' status : {status}")
                    if database_update and transaction_type:
                        FireblocksTransfer(transaction, transaction_type).to_database(self.database)
                    time.sleep(timeout)

            if status == "COMPLETED":
                log(f"[{self}] Transfer '{transaction_id}' completed")
            else:
                log(f"[{self}] Transfer '{transaction_id}' not completed : {status}")
        else:
            ts = int(datetime.datetime.now().timestamp()*1000)
            transaction = {
                "id": "FAKE-"+"".join(random.choice(string.ascii_lowercase + string.digits) for i in range(32)),
                "createdAt": ts,
                "lastUpdated": ts,
                "source": {
                    "type": src_type,
                    "name": src_name,
                },
                "destination": {
                    "type": dest_type,
                    "name": dest_name,
                },
                "amount": amount,
                "fee": -1,
                "networkFee": -1,
                "status": "COMPLETED",
                "txHash": "",
                "subStatus": "CONFIRMED",
                "feeCurrency": symbol,
            }

        return transaction

    def __binance_spot_to_margin_transfer(self, strategy, symbol, amount, fake_run=False):
        if symbol == "USDT_ERC20":
            symbol = "USDT"
        try:
            log(f"[{strategy}] Transfer {amount} {symbol} from SPOT account to MARGIN account")
            if not fake_run:
                response = strategy.get_api_connector().connector.spot_to_margin_transfer(symbol, amount)
                log(f"[{strategy}] {response}")
        except Exception as e:
            log(f"[{strategy}] Exception transfering from SPOT account to MARGIN account : {type(e)} - {e}")

    def __binance_margin_to_spot_transfer(self, strategy, symbol, amount, fake_run=False):
        if symbol == "USDT_ERC20":
            symbol = "USDT"
        try:
            log(f"[{strategy}] Transfer {amount} {symbol} from MARGIN account to SPOT account")
            if not fake_run:
                response = strategy.get_api_connector().connector.margin_to_spot_transfer(symbol, amount)
                log(f"[{strategy}] {response}")
        except Exception as e:
            log(f"[{strategy}] Exception transfering from MARGIN account to SPOT accoun : {type(e)} - {e}")

    def __binance_redeem(self, strategy, symbol, amount, fake_run=False):
        if symbol == "USDT_ERC20":
            symbol = "USDT"
        try:
            if symbol != "USDT":
                return

            product_id = strategy.get_api_connector().connector.get_flexible_earn_product_list('USDT')
            flexible_earn_position = strategy.get_api_connector().connector.get_flexible_earn_position('USDT')
            flexible_earn_position_amount = flexible_earn_position['size']
            size_reedem = round(min(float(flexible_earn_position_amount), amount), 0)

            if size_reedem > 10:
                log(f"[{strategy}] Redeem {size_reedem} on product id {product_id}")
                if not fake_run:
                    response = strategy.get_api_connector().connector.redeem_flexible_earn(product_id, amount=size_reedem)
                    log(f"[{strategy}] {response}")

        except Exception as e:
            log(f"[{strategy}] Exception redeeming USDT : {type(e)} - {e}")

    def __make_binance_link_transfer(self, binance_link_connector, symbol, amount, from_subaccount, to_subaccount, database_update=True, wait=True, timeout=1, transaction_type=None, fake_run=False):
        if symbol == "USDT_ERC20":
            symbol = "USDT"
        log(f"[{self}] Create transfer of '{amount}' {symbol} from '{from_subaccount if from_subaccount else 'MASTER'}' to '{to_subaccount if to_subaccount else 'MASTER'}'")

        if not fake_run:
            custom_id = "".join(random.choice(string.ascii_lowercase + string.digits) for i in range(32))
            transaction = binance_link_connector.make_transfer(symbol, amount, from_subaccount, to_subaccount, custom_id=custom_id)
            log(f"[{self}] {transaction}")

            if wait:
                status = ""
                while status not in ["SUCCESS", "FAILURE"]:
                    log(f"[{self}] Check transfer '{custom_id}' status")
                    transactions = binance_link_connector.get_transfer_history(custom_id=custom_id)
                    if len(transaction) > 0:
                        transaction = transactions[0]
                        status = transaction["status"]
                        log(f"[{self}] Transfer '{custom_id}' status : {status}")
                        #if database_update and transaction_type:
                        #    BinanceLinkTransfer(custom_id, transaction, transaction_type).to_database(self.database)
                    time.sleep(timeout)

                transaction["amount"] = transaction["qty"]

                if status == "SUCCESS":
                    transaction["status"] = "COMPLETED"
                    log(f"[{self}] Transfer '{custom_id}' completed")
                else:
                    log(f"[{self}] Transfer '{custom_id}' not completed : {status}")
        else:
            ts = int(datetime.datetime.now().timestamp()*1000)
            transaction = {
                "id": "FAKE-"+"".join(random.choice(string.ascii_lowercase + string.digits) for i in range(32)),
                "createdAt": ts,
                "lastUpdated": ts,
                "source": from_subaccount,
                "destination": to_subaccount,
                "amount": amount,
                "fee": 0,
                "networkFee": 0,
                "status": "COMPLETED",
                "txHash": "",
                "subStatus": "CONFIRMED",
                "feeCurrency": symbol,
            }

        return transaction

    def __transfer_round_size(self, exchange, asset, size):
        rounded_size = size
        if exchange == Exchanges.DERIBIT:
            if asset == "USDC":
                rounded_size = round(size, 2)
            elif asset == "ETH":
                rounded_size = round(size, 6)
            elif asset == "BTC":
                rounded_size = round(size, 8)
        return rounded_size

    def __str__(self):
        return f"{self.id} - {self.name}"


