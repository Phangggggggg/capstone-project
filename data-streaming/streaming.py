import json
import threading
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
import websocket



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def load_symbol_lists():
    return pd.read_csv('data-streaming/data/nasdaq_10.csv',encoding='utf-8')['Ticker'].to_list()

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


def on_message(ws, message):
    message = json.loads(message)
    if message and message['type'] == 'trade':
        data = message['data']
        if data and len(data) > 0:
            for info in data:
                stock_producer.produce_message(key=data['s'],value=info)
            stock_producer.flush()
            logger.info(f"{stock_producer.get_delivered_count()} messages were produced to topic {STOCK_TOPIC}!")

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    for symbol in symbol_lists:
        listen_message = {"type": "subscribe", "symbol": symbol}
        ws.send(json.dumps(listen_message))
    
def fetch_stock():
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={FINHUB_API_KEY}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()


                
if __name__ == '__main__':
    load_dotenv()
    FINHUB_API_KEY = os.environ.get("FINHUB_API_KEY")
    VISA_TOPIC = os.environ.get("VISA_TOPIC")
    STOCK_TOPIC = os.environ.get("STOCK_TOPIC")
    CONFIG_FILE = os.environ.get("CONFIG_FILE")
    START_DATE = "2023-08-10"
    END_DATE = datetime.now().date().strftime('%Y-%m-%d')
    
    finhub_client = FinnhubClient(api_key=FINHUB_API_KEY)
    
    visa_producer = KafkaProducer(config_file=CONFIG_FILE,topic=VISA_TOPIC)
    stock_producer = KafkaProducer(config_file=CONFIG_FILE,topic=STOCK_TOPIC)
    
    setup_tracking_database()
    websocket.enableTrace(True)
    stock_thread = threading.Thread(target=fetch_stock, daemon=True)
    stock_thread.start()

    schedule.every(1).minutes.do(fetch_visa)
    while True:
        schedule.run_pending()
        time.sleep(1)
