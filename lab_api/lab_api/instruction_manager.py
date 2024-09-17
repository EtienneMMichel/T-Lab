import pandas as pd

class InstructionManager():
    def __init__(self, database):
        self.database = database

    def display_instructions(self, instruction_table):
        df = self.database.getTable(instruction_table)
        return df.to_dict(orient='records')
    

        
    def is_instruction_exist(self, instruction_table, instruction):
        instruction.pop("id", None)

        instruction_already_exist = False
        instruction_id = None
        instructions_inside_db = self.database.getTable(instruction_table)
        instructions_inside_db = instructions_inside_db.to_dict(orient='records')
        for existing_instruction in instructions_inside_db:
            id = existing_instruction.pop("id", None)
            if instruction == existing_instruction:
                instruction_already_exist = True
                instruction_id = id
        if instruction_id is None:
            instruction_id = len(instructions_inside_db) + 1
        return instruction_already_exist, instruction_id
        
    def add_instruction(self, instruction_table, instruction):
        instruction_already_exist,instruction_id = self.is_instruction_exist(instruction_table, instruction)
        instruction["id"] = instruction_id
        if not instruction_already_exist:
            self.database.append_to_table(instruction_table, pd.DataFrame([instruction]))
            return "Success"
        return "Already exist"

    def remove_instruction(self, instruction_table, instruction):
        instruction_already_exist, instruction_id = self.is_instruction_exist(instruction_table, instruction)
        if instruction_already_exist:
            instructions_inside_db = self.database.getTable(instruction_table)
            instructions_inside_db.drop(index=instructions_inside_db[instructions_inside_db["id"] == instruction_id].index, inplace=True)
            self.database.replace_table(instruction_table,instructions_inside_db)
            return "Success"
        return "Instruction not exist"
    
    def remove_all_instructions(self, instruction_table):
        df = self.database.getTable(instruction_table)
        self.database.replace_table(instruction_table,pd.DataFrame(columns=df.columns))
        return "Success"