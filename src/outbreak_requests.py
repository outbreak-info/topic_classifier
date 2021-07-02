import requests
import time

def get(*args, **kwargs):
    time.sleep(1)
    return requests.get(*args, **kwargs)

def post(*args, **kwargs):
    time.sleep(1)
    return requests.post(*args, **kwargs)
