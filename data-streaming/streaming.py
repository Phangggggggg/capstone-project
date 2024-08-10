import json
import time
from functions.producer import KafkaProducer
from functions.finhub_api import FinnhubClient
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from functools import lru_cache
from database.last_fetch import FetchTracker,setup_tracking_database,get_last_fetch_date,update_last_fetch_date
import logging
import schedule


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def load_symbol_lists():
    return pd.read_csv('data-streaming/data/nasdaq-listed-symbols.csv')['Symbol'].to_list()

symbol_lists = load_symbol_lists()

def fetch_visa():
    for symbol in symbol_lists:
        try:
            last_fetch_date = get_last_fetch_date(symbol)
            start_date = last_fetch_date if last_fetch_date else START_DATE
            end_date = END_DATE

            data = finhub_client.get_visa_data(symbol=symbol, start_date=start_date, end_date=end_date)
            update_last_fetch_date(symbol, end_date)
            
            if data and len(data['data']) > 0:
                data_list = data['data']
                
                if not isinstance(data_list, list):
                    logger.warning(f"Invalid data format for symbol: {symbol}")
                    continue

                for record in data_list:
                    visa_producer.produce_message(key=symbol,message=record)
                
                visa_producer.flush()
                logger.info(f"{visa_producer.get_delivered_count()} messages were produced to topic {VISA_TOPIC}!")
            else:
                logger.warning(f"No data fetched for symbol: {symbol}")
                
        except Exception as e:
            logger.error(f"Error processing symbol {symbol}: {e}")

def fetch_stock():
    for symbol in symbol_lists:
        data = finhub_client.get_stock_data(symbol=symbol)
        if data:
            stock_producer.produce_message(key=symbol,message=data)
            stock_producer.flush()
                
if __name__ == '__main__':
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    VISA_TOPIC = os.environ.get("VISA_TOPIC")
    STOCK_TOPIC = os.environ.get("STOCK_TOPIC")
    CONFIG_FILE = os.environ.get("CONFIG_FILE")
    START_DATE = "2023-08-10"
    END_DATE = datetime.now().date().strftime('%Y-%m-%d')
    
    finhub_client = FinnhubClient(api_key=API_KEY)
    visa_producer = KafkaProducer(config_file=CONFIG_FILE,topic=VISA_TOPIC)
    stock_producer = KafkaProducer(config_file=CONFIG_FILE,topic=STOCK_TOPIC)
    
    setup_tracking_database()
    
    schedule.every().day.at("14:06").do(fetch_visa)

    schedule.every(1).minutes.do(fetch_stock)

    while True:
        schedule.run_pending()
        time.sleep(10) 