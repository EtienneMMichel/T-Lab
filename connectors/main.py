from connectors.core import Core
import redis
import json
# import yaml
# import sys

from dotenv import load_dotenv
import os

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST')


SUB_KEY = "connector_request"


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
                instructions = json.loads(message["data"])
            except TypeError:
                instructions = None
            if isinstance(instructions, dict):
                if instructions["from"] == "collector":
                    core_model.process_instructions(instructions["data"])
                    
        core_model.send_data(r)
                


if __name__ == "__main__":
    # global_config = yaml.safe_load(open(sys.argv[1], "r"))
    r = redis.Redis(REDIS_HOST, 6379, charset="utf-8", decode_responses=True)
    print("SETUP INSTANCE")
    core_model = Core()
    print("STREAM")


    stream(core_model,r)