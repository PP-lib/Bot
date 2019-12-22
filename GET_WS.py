	
# pip install websocket-client
import websocket
import json
import pandas as pd
import threading
import time
from datetime import datetime as dt
from datetime import timedelta
CHANNEL = "lightning_executions_FX_BTC_JPY"

df_new = pd.DataFrame(index=['datetime'],
                    columns=['id', 
                            'side', 
                            'price', 
                            'size', 
                            'exec_date', 
                            'buy_child_order_acceptance_id', 
                            'sell_child_order_acceptance_id'])

def on_message(ws, message):
    global df_new
    message = json.loads(message)
    if message["method"] == "channelMessage":
        params_message = message["params"]["message"]
        df_new = pd.DataFrame(params_message)
        df_new['exec_date'] = pd.to_datetime(df_new['exec_date']) + timedelta(hours=9)
        df_new = df_new.append(df_new,sort=True)
        df_new.index = df_new['exec_date']

        print(df_new.tail(5))
        print(len(df_new))
        df_new.to_csv('./datas/WS.csv', mode='a', header=False)

        return df_new

def on_open(ws):
    ws.send(json.dumps({"method": "subscribe",
                        "params": {"channel": CHANNEL}}))

def main():
    ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc",
                                on_message=on_message, on_open=on_open)
    
    ws.run_forever()
 
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit()
