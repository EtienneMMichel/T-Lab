import threading

class Singleton(type):
    _lock = threading.Lock()
    _instance = None
    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance