from .utils import Database
import pandas as pd
from datetime import datetime
import json
TABLE = "orderbooks"

class Core():
    def __init__(self, database_config) -> None:
        self.database = Database(ip=database_config["ip"],
                                 user=database_config["user"],
                                 password=database_config["password"],
                                 dbName=database_config["dbName"])
        self.database.connect()
        self.current_instructions = None

    def store(self, in_data):
        in_data = in_data["data"]
        df = pd.DataFrame({
            "platform_id":in_data["platform_id"],
            "symbol":in_data["binance_symbol"],
            "sell":json.dumps(in_data["sell"]),
            "buy":json.dumps(in_data["buy"]),
            "date":str(datetime.now().timestamp())
        })
        self.database.append_to_table(TABLE, df)
    
    def check_instructions(self):
        collector_instructions = self.database.getTable("collector_instructions")
        if (self.current_instructions is None) or (not collector_instructions.equals(self.current_instructions)):
            print("INSTRUCTIONS UPDATE")
            self.current_instructions = collector_instructions.copy(deep=True)
            res = collector_instructions.to_dict(orient='records')
            if len(res) > 0:
                return res

        return None