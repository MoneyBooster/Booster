from ast import Pass
from pathlib import Path
import fire
import requests
import io
import datetime
import os
import time

import requests
import numpy as np
import pandas as pd
import sys
import abc
from abc import ABC

from loguru import logger
from dateutil.tz import tzlocal

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
API_KEY='MLG92LA5SYPM8357'

#import qlib
from qlib.constant import REG_US as REGION_US
from qlib.utils import code_to_fname, fname_to_code, exists_qlib_data

from base import BaseCollector, BaseNormalize, BaseRun, Normalize
from utils import deco_retry, get_us_stock_symbols

INDEX_BENCH_URL = "https://www.alphavantage.co/query?apikey={apikey}&function=TIME_SERIES_INTRADAY&symbol={ticker}&interval={interval}&outputsize=full&datatype=csv"

class AlphaCollector(BaseCollector):
    retry = 5  # Configuration attribute.  How many times will it try to re-request the data if the network fails.

    INTERVAL_5min = "5min"
    DEFAULT_START_DATETIME_5MIN = pd.Timestamp(datetime.datetime.now() - pd.Timedelta(days=5 * 6 - 1)).date()

    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1min",
        max_workers=4,
        max_collector_count=2,
        delay=0,
        check_data_length: int = None,
        limit_nums: int = None,
    ):
        """

        Parameters
        ----------
        save_dir: str
            stock save dir
        max_workers: int
            workers, default 4
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0
        interval: str
            freq, value from [1min, 5min], default 5min
        start: str
            start datetime, default None
        end: str
            end datetime, default None
        check_data_length: int
            check data length, by default None
        limit_nums: int
            using for debug, by default None
        """
        super(AlphaCollector, self).__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )

        self.init_datetime()

    def init_datetime(self):
        if self.interval == self.INTERVAL_1min:
            self.start_datetime = max(self.start_datetime, self.DEFAULT_START_DATETIME_1MIN)
        elif self.interval == self.INTERVAL_5min:
            self.start_datetime = max(self.start_datetime, self.DEFAULT_START_DATETIME_5MIN)

        else:
            raise ValueError(f"interval error: {self.interval}")

        self.start_datetime = self.convert_datetime(self.start_datetime, self._timezone)
        self.end_datetime = self.convert_datetime(self.end_datetime, self._timezone)

    @staticmethod
    def convert_datetime(dt: [pd.Timestamp, datetime.date, str], timezone):
        try:
            dt = pd.Timestamp(dt, tz=timezone).timestamp()
            dt = pd.Timestamp(dt, tz=tzlocal(), unit="s")
        except ValueError as e:
            pass
        return dt

    @property
    @abc.abstractmethod
    def _timezone(self):
        raise NotImplementedError("rewrite get_timezone")

    @staticmethod
    def get_data_from_remote(symbol, interval, start, end, show_1min_logging: bool = False):
        pass

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> pd.DataFrame:
        # Step 3. Get stock price (intraday)
        # try:
        _result = self._stockPriceIntraday(symbol, folder='./Data/IntradayUS')

        return pd.DataFrame() if _result is None else _result

        time.sleep(2)
        # except:
        #     pass

    @abc.abstractmethod
    def _stockPriceIntraday(self,ticker, folder):
        raise NotImplementedError("rewrite stockPriceIntraday")

    def collector_data(self):
        """collector data"""
        super(AlphaCollector, self).collector_data()
        self.download_index_data()

    @abc.abstractmethod
    def download_index_data(self):
        """download index data"""
        raise NotImplementedError("rewrite download_index_data")


class AlphaCollectorUS(AlphaCollector, ABC):
    def get_instrument_list(self):
        # logger.info("get US stock symbosls......")
        # symbols = get_us_stock_symbols() + [
        #     "^GSPC",
        #     "^NDX",
        #     "^DJI",
        # ]
        # logger.info(f"get {len(symbols)} symbols.")
        # return symbols

        logger.info("get US stock symbosls......")
       # file =  '../Data/Nasdaq100/' +'nasdaq100' +'.csv'
    #file = 'D:/MoneyBooster/Booster/Booster/Scripts/Data_Collector/AlphaVantage/Data/Nasdaq100/nasdaq100.csv'
        file = '~/.qlib/nasdaq100.csv'
        #if os.path.exists(file):
        symbols = pd.read_csv(file)['Symbol'].tolist()
        logger.info(f"get {len(symbols)} symbols.")
        return symbols

    #@staticmethod


    def _stockPriceIntraday(self,ticker, folder):
        @deco_retry(retry_sleep=self.delay, retry=self.retry)
        def _dataframeFromUrl(url):
            self.sleep()
            dataString = requests.get(url).content
            parsedResult = pd.read_csv(io.StringIO(dataString.decode('utf-8')), index_col=0)
            return parsedResult

        # Step 1. Get data online
        url = INDEX_BENCH_URL.format(ticker=ticker,interval=self.interval,apikey=API_KEY)
        intraday = _dataframeFromUrl(url)
        return intraday
        # # Step 2. Append if history exists
        # file = folder+'/'+ticker+'.csv'
        # if os.path.exists(file):
        #     history = pd.read_csv(file, index_col=0)
        #     intraday.append(history)

        # # Step 3. Inverse based on index
        # intraday.sort_index(inplace=True)

        # # Step 4. Save
        # intraday.to_csv(file)
        # print ('Intraday for ['+ticker+'] got.')
        
    def download_index_data(self):
        Pass

        tickers = self.instrument_list
        # Step 3. Get stock price (intraday)
        for i, ticker in enumerate(tickers):
            try:
                print ('Intraday', i, '/', len(tickers))
                self._stockPriceIntraday(ticker, folder='./Data/IntradayUS')
                time.sleep(2)
            except:
                pass
        print ('Intraday for all stocks got.')

    def normalize_symbol(self, symbol):
        return code_to_fname(symbol).upper()

    @property
    def _timezone(self):
        return "America/New_York"

class AlphaCollectorUS1min(AlphaCollectorUS):
    pass


class AlphaCollectorUS5min(AlphaCollectorUS):
    pass


class Run(BaseRun):
    def __init__(self,apikey='MLG92LA5SYPM8357', source_dir=None, normalize_dir=None, max_workers=1, interval="1min", region=REGION_US):
        """

        Parameters
        ----------
        source_dir: str
            The directory where the raw data collected from the Internet is saved, default "Path(__file__).parent/source"
        normalize_dir: str
            Directory for normalize data, default "Path(__file__).parent/normalize"
        max_workers: int
            Concurrent number, default is 1; when collecting data, it is recommended that max_workers be set to 1
        interval: str
            freq, value from [1min, 5min], default 5min
        region: str
            region, value from ["CN", "US", "BR"], default "CN"
        """
        super().__init__(source_dir, normalize_dir, max_workers, interval)
        self.region = region
        self.apikey = apikey

    @property
    def collector_class_name(self):
        return f"AlphaCollector{self.region.upper()}{self.interval}"

    @property
    def normalize_class_name(self):
        return f"AlphaNormalize{self.region.upper()}{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=0.5,
        start=None,
        end=None,
        check_data_length=None,
        limit_nums=None,
    ):
        """download data from Internet

        Parameters
        ----------
        max_collector_count: int
            default 2
        delay: float
            time.sleep(delay), default 0.5
        start: str
            start datetime, default "2000-01-01"; closed interval(including start)
        end: str
            end datetime, default ``pd.Timestamp(datetime.datetime.now() + pd.Timedelta(days=1))``; open interval(excluding end)
        check_data_length: int
            check data length, if not None and greater than 0, each symbol will be considered complete if its data length is greater than or equal to this value, otherwise it will be fetched again, the maximum number of fetches being (max_collector_count). By default None.
        limit_nums: int
            using for debug, by default None

        Notes
        -----
            check_data_length, example:
                daily, one year: 252 // 4
                us 1min, a week: 6.5 * 60 * 5
                cn 1min, a week: 4 * 60 * 5

        Examples
        ---------
            # get daily data
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1d
            # get 1m data
            $ python collector.py download_data --source_dir ~/.qlib/stock_data/source --region CN --start 2020-11-01 --end 2020-11-10 --delay 0.1 --interval 1m
        """
        super(Run, self).download_data(max_collector_count, delay, start, end, check_data_length, limit_nums)

        # Step 1. Get ticker list online
        # tickersRawData = __dataframeFromUrl('http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download')
        # tickers = tickersRawData.index.tolist()

        # # Step 2. Save the ticker list to a local file
        # dateToday = datetime.datetime.today().strftime('%Y%m%d')
        # file = '../02. Data/00. TickerListUS/TickerList'+dateToday+'.csv'
        # tickersRawData.to_csv(file)
        # print ('Tickers saved.')

    def normalize_data(
        self,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
        end_date: str = None,
        qlib_data_1d_dir: str = None,
    ):
        """normalize data

        Parameters
        ----------
        date_field_name: str
            date field name, default date
        symbol_field_name: str
            symbol field name, default symbol
        end_date: str
            if not None, normalize the last date saved (including end_date); if None, it will ignore this parameter; by default None
        qlib_data_1d_dir: str
            if interval==1min, qlib_data_1d_dir cannot be None, normalize 1min needs to use 1d data;

                qlib_data_1d can be obtained like this:
                    $ python scripts/get_data.py qlib_data --target_dir <qlib_data_1d_dir> --interval 1d
                    $ python scripts/data_collector/yahoo/collector.py update_data_to_bin --qlib_data_1d_dir <qlib_data_1d_dir> --trading_date 2021-06-01
                or:
                    download 1d data, reference: https://github.com/microsoft/qlib/tree/main/scripts/data_collector/yahoo#1d-from-yahoo

        Examples
        ---------
            $ python collector.py normalize_data --source_dir ~/.qlib/stock_data/source --normalize_dir ~/.qlib/stock_data/normalize --region cn --interval 1d
            $ python collector.py normalize_data --qlib_data_1d_dir ~/.qlib/qlib_data/cn_data --source_dir ~/.qlib/stock_data/source_cn_1min --normalize_dir ~/.qlib/stock_data/normalize_cn_1min --region CN --interval 1min
        """
        if self.interval.lower() == "1min":
            if qlib_data_1d_dir is None or not Path(qlib_data_1d_dir).expanduser().exists():
                raise ValueError(
                    "If normalize 1min, the qlib_data_1d_dir parameter must be set: --qlib_data_1d_dir <user qlib 1d data >, Reference: https://github.com/microsoft/qlib/tree/main/scripts/data_collector/yahoo#automatic-update-of-daily-frequency-datafrom-yahoo-finance"
                )
        super(Run, self).normalize_data(
            date_field_name, symbol_field_name, end_date=end_date, qlib_data_1d_dir=qlib_data_1d_dir
        )


if __name__ == "__main__":
    fire.Fire(Run)
    # RInstance = Run()
    # RInstance.download_data()




