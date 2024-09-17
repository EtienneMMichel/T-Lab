PUB_KEY_COLLECTOR = "collector"

class Core():
    def __init__(self) -> None:
        pass

    def process_instructions(self, instructions):
        """
        change instructions given instructions sent from collector
        """
        raise NotImplementedError()
    
    def send_data(self, r):
        """
        sending market data according to instructions
        """
        raise NotImplementedError()
