from pydantic import BaseModel

class InstructionBody(BaseModel):
    table:str
    instruction: dict

class DisplayBody(BaseModel):
    table:str

class CollectorInstruction:
    exchange:int
    symbol:str
    rate:int
    data_type:str

