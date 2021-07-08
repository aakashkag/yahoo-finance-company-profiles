import pandas as pd
import traceback
import multiprocessing.pool
import yfinance as yf
import os
from timeit import default_timer as timer
import click
import json


class TickerCraler:

    def __init__(self, input_column):
        self.input_column = input_column

    def save_to_file(self, TickerInfo, Ticker):
        try:
            with open(f"./data/Output/{Ticker}.json", "w") as write_file:
                json.dump(TickerInfo.info, write_file, indent=4)
        except:
            traceback.print_exc()

    def website_profile_downloader(self, obj):
        print('obj===>',obj)
        TICKER = obj[self.input_column]
        print('Ticker==>',TICKER)
        TickerInfo = yf.Ticker(TICKER)
        self.save_to_file(TickerInfo, TICKER)

@click.command()
@click.option('--nprocesses', default=5, help='Mention number of processes to run in parallel(By default 10 processes)')
@click.option('--input_file', help='Input file name')
@click.option('--input_column', help='input column name')
@click.option('--crawl_first_n_tickers', default=-1)

def start_downloader(nprocesses, input_file,input_column, crawl_first_n_tickers):
    try:
        start = timer()
        df = pd.read_excel(input_file,sheet_name='Stock', skiprows=3)
        if crawl_first_n_tickers >- 1:
            df = df[0:crawl_first_n_tickers]
        print('Data shape', df.shape, df.head())

        df[input_column] = df[input_column].str.strip()
        df[input_column] = df[input_column].str.lower()

        # Filter which is now already scrapped

        existing_files = os.listdir("./data/Output/")
        existing_files = [file.replace('.json', '') for file in existing_files]
        df = df[~df[input_column].isin(existing_files)]
        seeds = df.to_dict('records')
        print(f'Total Input unique seeds:{len(seeds)}')
        
        obj = TickerCraler(input_column)
        pool = multiprocessing.pool.ThreadPool(processes=nprocesses)
        return_list = pool.map(obj.website_profile_downloader, seeds, chunksize=1)
        pool.close()
        end = timer()
        print('<=========== Crawling done Now combining result:) ===============>')
        print("Time taken:", end - start)
    except:
        traceback.print_exc()

# Load and Start Crawler
if __name__ == '__main__':
    start_downloader()

#python3 crawler.py --input_file  --input_column website --nprocesses 5 --crawl_first_n_tickers 100
