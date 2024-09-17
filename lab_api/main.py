from lab_api import collector

from lab_api.core import Core
from dotenv import load_dotenv
import os

load_dotenv()

IP = os.getenv('IP')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DBNAME = os.getenv('DBNAME')


if __name__ == "__main__":
    database_config = {
        "ip":IP,
        "user":USER,
        "password":PASSWORD,
        "dbName":DBNAME
    }
    core_model = Core(database_config=database_config)
    
    instruction = {"exchange": collector.Exchanges.BINANCE.value,
                   "symbol":"SOL_USDT",
                   "rate":collector.Rate.min_5.value,
                }
    print(core_model.display_instructions(instruction_table="collector_instructions"))
    core_model.add_instruction(instruction_table="collector_instructions", instruction=instruction)
    print(core_model.display_instructions(instruction_table="collector_instructions"))
    