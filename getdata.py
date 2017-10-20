import os
import time
from datetime import datetime

import pandas as pd

ONE_DAY_SECONDS = 24 * 3600
TWO_HOURS_SECONDS = 2 * 3600
NOW = datetime.utcnow().timestamp()

START_DATE = NOW - int(os.environ['DAY']) * ONE_DAY_SECONDS
DELAY_SECONDS = 0.2

OVERRIDE_PAIRS = False
PAIR_LIST = ["BTC_ETH"]

FETCH_URL = "https://poloniex.com/public?command=returnChartData&currencyPair=%s&start=%d&end=%d&period=300"
DATA_DIR = "data"
COLUMNS = ["date","high","low","open","close","volume","quoteVolume","weightedAverage"]

def get_data(pair, start_time, end_time):
    datafile = os.path.join(DATA_DIR, pair+".csv")
    timefile = os.path.join(DATA_DIR, pair)

    if os.path.exists(timefile) and os.path.exists(datafile):
        newfile = False
        start_time = int(open(timefile).readline()) + 1
    else:
        newfile = True

    if NOW - start_time >= TWO_HOURS_SECONDS/8:
        url = FETCH_URL % (pair, start_time, end_time)
        print("Get %s from %d to %d" % (pair, start_time, end_time))

        df = pd.read_json(url, convert_dates=False)

        #import pdb;pdb.set_trace()

        if df["date"].iloc[-1] == 0:
            print("No data.")
            return

        end_time = df["date"].iloc[-1]
        ft = open(timefile,"w")
        ft.write("%d\n" % end_time)
        ft.close()
        outf = open(datafile, "a")
        if newfile:
            df.to_csv(outf, index=False, columns=COLUMNS)
        else:
            df.to_csv(outf, index=False, columns=COLUMNS, header=False)
        outf.close()
        print("Finish.")
        time.sleep(DELAY_SECONDS)
    else:
        print('skipping', pair)


def main():
    if not os.path.exists(DATA_DIR):
        os.mkdir(DATA_DIR)

    df = pd.read_json("https://poloniex.com/public?command=return24hVolume")
    pairs = [pair for pair in df.columns if pair.startswith('BTC')] if not OVERRIDE_PAIRS else PAIR_LIST
    print(pairs)

    end_time = START_DATE + ONE_DAY_SECONDS
    
    for pair in pairs:
        print(os.environ['DAY'])
        get_data(pair, START_DATE, end_time)

if __name__ == '__main__':
    main()
