from datetime import datetime, timedelta
from . import utils
from connectors.crypto.connector.common.platforms import Exchange
import json

PUB_KEY_COLLECTOR = "collector"

class Core():
    def __init__(self) -> None:
        self.instructions = None
        self.last_sending = {}

    def process_instructions(self, instructions):
        """
        change instructions given instructions sent from collector
        """
        self.instructions = instructions

    def get_platform_symbol(self, instruction_symbol, platform_id):
        if platform_id == utils.Exchanges.BINANCE.value:
            return instruction_symbol
        else:
            raise NotImplementedError()
    
    def __fetch_data_from_exchange(self, instruction):
        if instruction["data_type"] == utils.DataType.ORDERBOOK.value:
            connector = Exchange(platform_id=instruction["exchange"]).connector("","")
            plateform_symbol = self.get_platform_symbol(instruction["symbol"], instruction["exchange"])
            orderbook = connector.get_orderbook(plateform_symbol)
            now = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            return {
                "platform_id":instruction["exchange"],
                "binance_symbol": instruction["symbol"],
                "buy": orderbook["buy"],
                "sell": orderbook["sell"],
                "timestamp": now,
                "created_at": now,
                "updated_at": now,
            }    

        else:
            raise NotImplementedError()       
            



    def __send_to_canal(self, instruction, data, r):
        data_to_send = {"data": data,
                        "instruction": instruction,
                        "from": "connectors",
                        }
        data_to_send = json.dumps(data_to_send, ensure_ascii=False)
        r.publish(PUB_KEY_COLLECTOR,data_to_send)
    
    def __process(self, instruction, r):
        """
        fetch data from connectors and send it back to collector 
        """
        self.last_sending[instruction["id"]] = datetime.now()
        data = self.__fetch_data_from_exchange(instruction)
        self.__send_to_canal(instruction, data, r)

    def send_data(self, r):
        """
        sending market data according to instructions and rate
        """
        if self.instructions:
            for instruction in self.instructions:
                if instruction["id"] in list(self.last_sending.keys()):
                    if datetime.now() - self.last_sending[instruction["id"]] >= timedelta(minutes=instruction["rate"]): # rate condition
                        self.__process(instruction, r)
                else:
                    #init
                    self.__process(instruction, r) 
                

