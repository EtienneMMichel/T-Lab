from .utils import Database


class Core():
    def __init__(self, database_config) -> None:
        self.database = Database(ip=database_config["ip"],
                                 user=database_config["user"],
                                 password=database_config["password"],
                                 dbName=database_config["dbName"])
        self.database.connect()
        self.current_instructions = None

    def store(self, in_data):
        raise NotImplementedError()
    
    def check_instructions(self):
        collector_instructions = self.database.getTable("collector_instructions")
        if (self.current_instructions is None) or (not collector_instructions.equals(self.current_instructions)):
            print("INSTRUCTIONS UPDATE")
            self.current_instructions = collector_instructions.copy(deep=True)
            res = collector_instructions.to_dict(orient='records')
            if len(res) > 0:
                return res

        return None