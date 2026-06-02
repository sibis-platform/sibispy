#!/usr/bin/env python3
import argparse
from datetime import datetime
import os
import urllib3

REDCAP_URL = os.getenv('REDCAP_API_URL', "https://ncanda.sri.com/redcap/api/")

def check_redcap_status():
    http = urllib3.PoolManager()
    print (f'{datetime.now().isoformat()} redcap_status_check for {REDCAP_URL}...', flush=True, end="")
    http.request("POST", REDCAP_URL)
    print ("SUCCESS")

if __name__ == "__main__":
    check_redcap_status()
