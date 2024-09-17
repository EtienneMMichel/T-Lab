import traceback
import threading

class ExceptionThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(ExceptionThread, self).__init__(*args, **kwargs)
        self.__stop_event = threading.Event()

    def run(self):
        self.exc = None
        self.ret = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e
        self.stop()

    def join(self, timeout=None):
        threading.Thread.join(self, timeout)
        if self.exc:
            raise self.exc
        return self.ret

    def stop(self):
        self.__stop_event.set()

    def is_stopped(self):
        return self.__stop_event.is_set()

def format_traceback(tb):
    traceback_string = ""
    for block in traceback.format_tb(tb):
        traceback_string += block
    return traceback_string

class StoppableThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(StoppableThread, self).__init__(*args, **kwargs)
        self.__stop_event = threading.Event()

    def stop(self):
        self.__stop_event.set()

    def is_stopped(self):
        return self.__stop_event.is_set()
