from fastapi import FastAPI
import uvicorn

from lab_api.utils import request_interface
from lab_api.core import Core
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_CONFIG = {
        "ip":os.getenv('IP'),
        "user":os.getenv('USER'),
        "password":os.getenv('PASSWORD'),
        "dbName":os.getenv('DBNAME')
    }


app = FastAPI(root_path="/prod")


@app.get("/")
async def root() -> dict:
    """
    **Dummy endpoint that returns 'hello world' example.**

    ```
    Returns:
        dict: 'hello world' message.
    ```
    """
    return {"message": "Hello World"}


@app.post("/add_instruction")
async def add_instruction(request:request_interface.InstructionBody) -> dict:
    core_model = Core(database_config=DATABASE_CONFIG) 
    status = core_model.add_instruction(instruction_table=request.table, instruction=request.instruction)
    return {"status": status}


@app.post("/remove_instruction")
async def remove_instruction(request:request_interface.InstructionBody) -> dict:
    core_model = Core(database_config=DATABASE_CONFIG) 
    status = core_model.remove_instruction(instruction_table=request.table, instruction=request.instruction)
    return {"status": status}

@app.post("/display_instructions")
async def display_instructions(request:request_interface.DisplayBody) -> dict:
    core_model = Core(database_config=DATABASE_CONFIG) 
    instructions = core_model.display_instructions(instruction_table=request.table)
    return {"status": "Success", "data":instructions}

@app.post("/remove_all_instructions")
async def remove_all_instructions(request:request_interface.DisplayBody) -> dict:
    core_model = Core(database_config=DATABASE_CONFIG) 
    status = core_model.remove_all_instructions(instruction_table=request.table)
    return {"status": status}

if __name__ == "__main__":
    uvicorn.run("app:app", port=5000, log_level="info")