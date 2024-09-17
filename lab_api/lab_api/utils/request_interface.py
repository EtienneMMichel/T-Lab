from pydantic import BaseModel

class InstructionBody(BaseModel):
    table:str
    instruction: dict

class CollectorInstruction:
    exchange:int
    symbol:str
    rate:int