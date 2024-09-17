from .utils import Database
from .instruction_manager import InstructionManager


class Core():
    def __init__(self, database_config) -> None:
        self.database = Database(ip=database_config["ip"],
                                 user=database_config["user"],
                                 password=database_config["password"],
                                 dbName=database_config["dbName"])
        self.database.connect()
        self.instruction_manager = InstructionManager(database=self.database)
        
    #----------------INSTRUCTION MANAGER-------------
    def display_instructions(self, instruction_table):
        return self.instruction_manager.display_instructions(instruction_table)
    
    def add_instruction(self, instruction_table, instruction):
        return self.instruction_manager.add_instruction(instruction_table, instruction)

    def remove_instruction(self, instruction_table, instruction):
        return self.instruction_manager.remove_instruction(instruction_table, instruction)

        
    