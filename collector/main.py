from collector.core import Core
import redis
import json
# import yaml
from dotenv import load_dotenv
import os

load_dotenv()

IP = os.getenv('IP')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DBNAME = os.getenv('DBNAME')


SUB_KEY = "collector"
PUB_KEY = "connector_request"

def stream(core_model, r):
    p = r.pubsub()
    p.psubscribe(SUB_KEY)
    while True:
        # listen to odd_request
        message = p.get_message()
        if message is not None and isinstance(message, dict) :
            # print(message["data"])
            # print(isinstance(message["data"], dict))
            try:
                in_data = json.loads(message["data"])
            except TypeError:
                in_data = None
            if isinstance(in_data, dict):
                core_model.store(in_data)

        instructions = core_model.check_instructions()
        if not instructions is None:
            data_to_send = {"data": instructions,
                            "from": "collector",
                            }
            data_to_send = json.dumps(data_to_send, ensure_ascii=False)
            r.publish(PUB_KEY,data_to_send)

                


if __name__ == "__main__":
    # global_config = yaml.safe_load(open(sys.argv[1], "r"))
    r = redis.Redis('localhost', 6379, charset="utf-8", decode_responses=True)
    print("SETUP INSTANCE")
    database_config = {
        "ip":IP,
        "user":USER,
        "password":PASSWORD,
        "dbName":DBNAME
    }
    core_model = Core(database_config=database_config)
    print("STREAM")


    stream(core_model,r)