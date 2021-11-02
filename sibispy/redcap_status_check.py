import argparse
from datetime import datetime
import os
import pytz
import urllib3

REDCAP_URL = "https://ncanda.sri.com/redcap/api/"

def check_redcap_status(log_filename):
    http = urllib3.PoolManager()
    tz = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(tz)

    try:
        http.request("POST", REDCAP_URL)
    except urllib3.exceptions.MaxRetryError:
        file_mode = "a"
        if not os.path.exists(log_filename):
            file_mode = "w"

        with open(log_filename, file_mode) as f:
            f.write(current_time.strftime("%m/%d/%Y, %H:%M:%S"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("logfile")
    args = parser.parse_args()
    check_redcap_status(args.logfile)
