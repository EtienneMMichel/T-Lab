from datetime import datetime, timedelta

PUB_KEY_COLLECTOR = "collector"

class Core():
    def __init__(self) -> None:
        self.instructions = None
        self.last_sending = {}

    def process_instructions(self, instructions):
        """
        change instructions given instructions sent from collector
        """
        self.instructions = instructions
    
    def __fetch_data_from_exchange(self, instruction):
        raise NotImplementedError()

    def __send_to_canal(self, instruction, data):
        raise NotImplementedError()
    
    def __process(self, instruction):
        """
        fetch data from connectors and send it back to collector 
        """
        self.last_sending[instruction["id"]] = datetime.now()
        data = self.__fetch_data_from_exchange(instruction)
        self.__send_to_canal(instruction, data)

    def send_data(self, r):
        """
        sending market data according to instructions and rate
        """
        for instruction in self.instructions:
            if instruction["id"] in list(self.last_sending.keys()):
                if datetime.now() - self.last_sending[instruction["id"]] >= timedelta(minutes=instruction["rate"]): # rate condition
                    self.__process(instruction)
            else:
                #init
                self.__process(instruction) 
                

