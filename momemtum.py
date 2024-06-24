import threading
import time
from rich.console import Console
from rich.logging import RichHandler
import logging
import websocket

import requests
import websocket
import json
import time
import os
from dotenv import load_dotenv
from urllib.parse import urlencode
import hmac
import hashlib

# 加载 .env 文件中的环境变量
dotenv_path = '.env'
load_dotenv(dotenv_path)

# 从环境变量中获取 API Key 和 API Secret
api_key = os.getenv('api_key')
api_secret = os.getenv('api_secret')

# 配置 logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
    handlers=[RichHandler()]
)
logger = logging.getLogger('rich')


def hashing(query_string):
    return hmac.new(
        api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()

accout_ws = None
trade_ws = None
price_ws = None
# 订阅账户数据流的函数
def account_stream():
    listen_key = ''
    BASE_URL = 'https://fapi.binance.com'

    url = f'{BASE_URL}/fapi/v1/listenKey'
    headers = {
        'X-MBX-APIKEY': api_key
    }
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        listen_key = response.json()['listenKey']


    def on_message(ws, message):
        logger.info(f"[blue]{message}[/]", extra={"markup": True})

    def on_error(ws, error):
        logger.error(error)

    def on_close(ws):
        logger.info('closed')

    global accout_ws
    accout_ws = websocket.WebSocketApp(f"wss://fstream.binance.com/ws/{listen_key}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    accout_ws.run_forever()

def trade_stream():
    global trade_ws

    def on_message(ws, message):
        logger.info(f"[red]{message}[/]", extra={"markup": True})
        timestamp = time.time_ns() / 1000000
        message_ = json.loads(message)
        if 'result' in message_:
            # logger.info(message_['result']['updateTime'] - timestamp)  
            logger.info(float(message_['result']['clientOrderId']) - float(message_['result']['updateTime']))


    def on_error(ws, error):
        logger.error(error)

    def on_close(ws):
        logger.info('closed')

    def on_open(ws):
        action()

    trade_ws = websocket.WebSocketApp("wss://ws-fapi.binance.com/ws-fapi/v1",
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)
    trade_ws.on_open = on_open

    trade_ws.run_forever()


def price_stream():
    global price_ws

    def on_error(ws, error):
        logger.error(error)

    def on_close(ws):
        logger.info('closed')


    price_ws = websocket.WebSocketApp(f"wss://ws-fapi.binance.com/ws-fapi/v1",
                                on_error=on_error,
                                on_close=on_close)
    # ws.on_open = on_open
    
    price_ws.run_forever()

action_orderid = {}
orderid_action ={}
orderid_detail = {}


condition = threading.Condition()

price = None


def get_price():

    global price_ws, condition, price


    def on_message(ws, message):
        with condition:
            global price
            price = message
            condition.notify()



    price_ws.on_message = on_message


    # 构建要发送的 JSON 数据
    payload = {
        "id": "51e2affb-0aba-4821-ba75-f2625006eb43",
        "method": "ticker.price",
        "params": {
            "symbol": "1000PEPEUSDC"
        }
    }

    # 将 JSON 数据转换为字符串并发送
    price_ws.send(json.dumps(payload))
    
    with condition:
        condition.wait()
        price_obj = json.loads(price)
        return float(price_obj['result']['price'])


current_action = 0

def create_order(newClientOrderId, positionSide, stopPrice, price, side, type, quantity):
    logger.warn(f'newClientOrderId: {newClientOrderId}, positionSide: {positionSide}, stopPrice: {stopPrice}, price: {price}, side: {side}, type: {type}')
    timestamp = int(time.time_ns() / 1000000)
    params = {
        "apiKey": api_key,
        "newClientOrderId": timestamp,
        "newOrderRespType": "RESULT",
        "positionSide": positionSide,

        "quantity": quantity,
        "side": side,
        "symbol": "1000PEPEUSDC",
        "timeInForce": "GTC",
        "timestamp": timestamp,
        "type": type
        
    }

    if type in ['STOP', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET', 'STOP_MARKET']:
        params['stopPrice'] = stopPrice

    if not type.endswith('MARKET'):
        params['price'] = price

    params = sorted(params.items())

    params = {k: v for k, v in params}

    params['signature'] = hashing(urlencode(params))

    # 构建要发送的 JSON 数据
    payload = {
        "id": newClientOrderId,
        "method": "order.place",
        "params": params,
    }
    trade_ws.send(json.dumps(payload))


def action():
    got_price = get_price()
    global current_action
    current_action += 1
    args = [
        [f'{current_action}-5', 'LONG', round(got_price * 1.0005, 7), 0, 'BUY', 'STOP_MARKET', int(5.5/got_price)],
        [f'{current_action}-6', 'LONG', round(got_price * 1.0025, 7), 0, 'SELL', 'TAKE_PROFIT_MARKET', int(5.5/got_price)],
        # [f'{current_action}-4', 'LONG', high_ask, round(high_ask * 0.998, 7), 'SELL', 'STOP_MARKET', int(5.5/high_ask)],

        [f'{current_action}-2', 'SHORT', round(got_price * 0.9995, 7), 0, 'SELL', 'STOP_MARKET', int(5.5/got_price)],
        [f'{current_action}-1', 'SHORT', round(got_price * 0.9975, 7), 0, 'BUY', 'TAKE_PROFIT_MARKET', int(5.5/got_price)],
        # [f'{current_action}-3', 'SHORT', low_bid, round(low_bid * 1.002, 7), 'SELL', 'STOP', int(5.5/low_bid)],

    ]
    for arg in args:
        create_order(*arg)
    


if __name__ == "__main__":
    t_account_stream = threading.Thread(target=account_stream, name='账户监听')
    t_account_stream.start()

    t_trade_stream = threading.Thread(target=trade_stream, name='交易执行')
    t_trade_stream.start()

    t_orderbook_stream = threading.Thread(target=price_stream, name='订单簿')
    t_orderbook_stream.start()
