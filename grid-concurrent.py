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
import os
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


# 每次 action 结束以后，需要重置以下全局变量
def reset():
    global start_time, open_orders, filled_times, got_price
    got_price = None
    open_orders = {
        '42': {
                'open-long-mid': False,
                'close-long-high': False,
                'close-long-low': False,
            
                'open-short-mid': False,
                'close-short-high': False,
                'close-short-low': False,
                'ended': True
            }
        }
    
    get_order_constants()
    
class OrderWorkder():
    baseline_price = -1
    timestamp = -1
    open_orders = None
    order_consts = None
    gain = 0.0008
    stop_loss = 0.03
    
    def __init__(self):
        self.baseline_price = get_price()
        self.timestamp = int(time.time_ns() / 10**6)
        self.open_orders = {
                'open-long-mid': False,
                'close-long-high': False,
                'close-long-low': False,
            
                'open-short-mid': False,
                'close-short-high': False,
                'close-short-low': False,
                'ended': False
            }
        self.order_consts = {
            'open-long-mid': [f'{self.timestamp}-open-long-mid', 'LONG', 0, round(got_price * (1 - self.gain), 7), 'BUY', 'LIMIT', int(5.5/got_price)],
            'close-long-high': [f'{self.timestamp}-close-long-high', 'LONG', 0, round(got_price * (1 ), 7), 'SELL', 'LIMIT', int(5.5/got_price)],
            'close-long-low': [f'{self.timestamp}-close-long-low', 'LONG', round(got_price * (1 - self.stop_loss), 7), 0, 'SELL', 'STOP_MARKET', int(5.5/got_price)],
            
            'open-short-mid': [f'{self.timestamp}-open-short-mid', 'SHORT', 0, round(got_price * (1 + self.gain), 7),  'SELL', 'LIMIT', int(5.5/got_price)],
            'close-short-low': [f'{self.timestamp}-close-short-low', 'SHORT', 0, round(got_price * (1 ), 7), 'BUY', 'LIMIT', int(5.5/got_price)],
            'close-short-high': [f'{self.timestamp}-close-short-high', 'SHORT',round(got_price * (1 + self.stop_loss), 7), 0, 'BUY', 'STOP_MARKET', int(5.5/got_price)]
        }
        
    def start(self):
        for key_ in [ 'open-long-mid', 'open-short-mid']:
            create_order(*(self.order_consts[key_]))
    
    
class AccountWS:
    def __init__(self):
        self.observers = []

        listen_key = ''
        BASE_URL = 'https://fapi.binance.com'

        url = f'{BASE_URL}/fapi/v1/listenKey'
        headers = {
            'X-MBX-APIKEY': api_key
        }
        response = requests.post(url, headers=headers)
        if response.status_code == 200:
            listen_key = response.json()['listenKey']

        global accout_ws
        
        accout_ws = websocket.WebSocketApp(f"wss://fstream.binance.com/ws/{listen_key}",
                                    on_message=self.on_message,
                                    on_error=print,
                                    )
        accout_ws.run_forever()

    def subscribe(self, observer):
        self.observers.append(observer)

    def unsubscribe(self, observer):
        self.observers.remove(observer)

    def notify(self, message):
        for observer in self.observers:
            observer.update(message)

    def on_message(self, ws, message):
        self.notify(message)


        


def get_order_constants():
    global got_price
    timestamp = int(time.time_ns() / 10**6)
    
    if not got_price:
        got_price = get_price()
    
    # 看公告， USDC 挂单零手续费的活动7月就要结束了。回归到标准 0.02% / 0.05% - Maker / Taker, 十分遗憾，所以 gain要设置的大一点
    gain = 0.0008
    stop_loss = 0.03
    
    return {
        'open-long-mid': [f'{timestamp}-open-long-mid', 'LONG', 0, round(got_price * (1 - gain), 7), 'BUY', 'LIMIT', int(5.5/got_price)],
        'close-long-high': [f'{timestamp}-close-long-high', 'LONG', 0, round(got_price * (1 ), 7), 'SELL', 'LIMIT', int(5.5/got_price)],
        'close-long-low': [f'{timestamp}-close-long-low', 'LONG', round(got_price * (1 - stop_loss), 7), 0, 'SELL', 'STOP_MARKET', int(5.5/got_price)],
        
        'open-short-mid': [f'{timestamp}-open-short-mid', 'SHORT', 0, round(got_price * (1 + gain), 7),  'SELL', 'LIMIT', int(5.5/got_price)],
        'close-short-low': [f'{timestamp}-close-short-low', 'SHORT', 0, round(got_price * (1 ), 7), 'BUY', 'LIMIT', int(5.5/got_price)],
        'close-short-high': [f'{timestamp}-close-short-high', 'SHORT',round(got_price * (1 + stop_loss), 7), 0, 'BUY', 'STOP_MARKET', int(5.5/got_price)]
    }


def calculate_total_profit(filled_orders):
    total_realized_pnl = 0
    total_commission = 0
    
    for order in filled_orders:
        total_realized_pnl += float(order['realizedPnl'])
        total_commission += float(order['commission'])
    
        total_profit = total_realized_pnl - total_commission
    return total_profit


# 订阅账户数据流的函数
def account_stream():
    global open_orders
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
        # logger.info(f"[blue]{message}[/]", extra={"markup": True})
        msg = json.loads(message)
        
        if msg['e'] == 'ORDER_TRADE_UPDATE':
            id = msg['o']['c']
            order_timestamp, open_or_close, long_or_short, high_or_low = id.split('-')
            
            if msg['o']['X'] == 'NEW':
                global filled_times
                # 这里主要是因为 trade_stream 一开始就会发送 4 次下单，这些下单不应该被响应
                filled_times += 1
                open_orders[f'{open_or_close}-{long_or_short}-{high_or_low}'] = True
            if msg['o']['X'] == 'FILLED' or msg['o']['X'] == 'EXPIRED':
                open_orders[f'{open_or_close}-{long_or_short}-{high_or_low}'] = False

            if msg['o']['X'] == 'FILLED':
                match f'{open_or_close}-{long_or_short}-{high_or_low}':
                    case 'open-long-mid':
                        create_order_unique('close-long-high')
                        create_order_unique('close-long-low')
                        
                    case 'open-short-mid':
                        create_order_unique('close-short-low')
                        create_order_unique('close-short-high')
                    case 'close-long-high' | 'close-short-low' | 'close-long-low' | 'close-short-high':
                        terminate()
                        reset()
                        time.sleep(2)
                        action()

    

    def on_error(ws, error):
        logger.error(error)

    def on_close(_, __, ___):
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
        pass


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
        "newClientOrderId": newClientOrderId,
        "newOrderRespType": "RESULT",
        "positionSide": positionSide,

        "quantity": quantity,
        "side": side,
        "symbol": "1000PEPEUSDC",
        "timeInForce": 'GTX' if type == 'LIMIT' else 'GTC',
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
    
def create_order_unique(instruction):
    order_consts = get_order_constants()
    print(open_orders)
    if not open_orders[instruction]:
        create_order(*order_consts[instruction])
        print('created', order_consts[instruction])


def action():
    order_constants = get_order_constants()
    
    keys = order_constants.keys()
    
    for key_ in keys:
        if key_ in [
                'open-long-mid',
                'open-short-mid',
                # 'close-long-high',
                # 'close-short-low'
            ]:
            create_order(*order_constants[key_])
    
start_time = int(time.time_ns() / 10**6)

def calculate_gain():
    params = {
        'symbol': '1000PEPEUSDC',
        'startTime': start_time,
        'timestamp': int(time.time_ns() / 10**6)
    }
    
    params['signature'] = hashing(urlencode(params))
    return  requests.get('https://fapi.binance.com/fapi/v1/userTrades', params=params, headers = {
        'X-MBX-APIKEY': api_key
    }).json()
    
    
def terminate():
    params = {
        'symbol': '1000PEPEUSDC',
        'timestamp': int(time.time_ns() / 10**6)
    }
    
    params['signature'] = hashing(urlencode(params))
    requests.delete('https://fapi.binance.com/fapi/v1/allOpenOrders', params=params, headers = {
        'X-MBX-APIKEY': api_key
    })
    
    with open('gain.txt', 'a+') as f:
        f.write(f'{datetime.now().strftime("%H:%M:%S")} 收益: {calculate_total_profit(calculate_gain())} \n')
        print(f'收益: {calculate_total_profit(calculate_gain())} \n')


if __name__ == "__main__":
    from datetime import datetime
    print(datetime.now().strftime("%H:%M:%S"))

    t_account_stream = threading.Thread(target=account_stream, name='账户监听')
    t_account_stream.start()
    
    t_price_stream = threading.Thread(target=price_stream, name='订单簿')
    t_price_stream.start()

    time.sleep(1)
    reset()
    t_trade_stream = threading.Thread(target=trade_stream, name='交易执行')
    t_trade_stream.start()
    