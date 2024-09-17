import threading
import datetime
# import syslog

USE_SYSLOG = False

mutex = threading.Lock()

def use_syslog(state=True):
    global USE_SYSLOG
    USE_SYSLOG = state

def get_timestamp():
    return datetime.datetime.now().strftime("%m-%d-%Y_%H-%M-%S.%f")

def log(message):
    global USE_SYSLOG
    global mutex
    if USE_SYSLOG:
        # syslog.syslog(message)
        print(message)
    else:
        with mutex:
            print("[{}] {}".format(get_timestamp(), message))
