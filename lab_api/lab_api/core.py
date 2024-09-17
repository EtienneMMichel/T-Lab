from .utils import Database
from pymysql.err import OperationalError
import pandas as pd

class Core():
    def __init__(self, database_config) -> None:
        self.database = Database(ip=database_config["ip"],
                                 user=database_config["user"],
                                 password=database_config["password"],
                                 dbName=database_config["dbName"])
        

    def display_instructions(self, instruction_table):
        return self.database.getTable(instruction_table)
        
    def is_instruction_exist(self, instruction_table, instruction):
        instruction.pop("id", None)

        instruction_already_exist = False
        instruction_id = None
        collector_instructions = self.database.getTable(instruction_table)
        collector_instructions = collector_instructions.to_dict(orient='records')
        for existing_instruction in collector_instructions:
            id = existing_instruction.pop("id", None)
            if instruction == existing_instruction:
                instruction_already_exist = True
                instruction_id = id
        if instruction_id is None:
            instruction_id = len(collector_instructions) + 1
        return instruction_already_exist, instruction_id
        
    def add_instruction(self, instruction_table, instruction):
        instruction_already_exist,instruction_id = self.is_instruction_exist(instruction_table, instruction)
        instruction["id"] = instruction_id
        if not instruction_already_exist:
            self.database.append_to_table(instruction_table, pd.DataFrame([instruction]))

    def remove_instruction(self, instruction_table, instruction):
        instruction_already_exist, instruction_id = self.is_instruction_exist(instruction_table, instruction)
        if instruction_already_exist:
            collector_instructions = self.database.getTable(instruction_table)
            collector_instructions.drop(index=collector_instructions[collector_instructions["id"] == instruction_id].index, inplace=True)
            self.database.replace_table(instruction_table,collector_instructions)

        
    