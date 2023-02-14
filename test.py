import pandas as pd
import ccxt.pro as ccxtpro
from asyncio import run
from kafka import KafkaProducer
import asyncio
import time
import json
import sys
import snowflake.connector


conn = snowflake.connector.connect(user='TM_CORE_DI_USER',
                                   password='9bWTt7NQ4QnmnA2',
                                   account='bta69289.us-east-1',
                                   warehouse='ASTRA_WH',
                                   database="CRYPTO_DB", schema='CCXT_PRO', role='TM_DATAINTEGRATION')

exchangeName = "binancecoinm"
query = f"""
WITH TMP AS(
SELECT PAIRS, BASESYMBOL, QUOTESYMBOL, TM_ID FROM CRYPTO_DB.CCXT_PRO.CCXT_TOKEN_MAP
WHERE EXCHANGE='binancecoinm' AND TM_ID IS NOT NULL AND QUOTESYMBOL IN ('USD', 'USDT'))

SELECT * FROM (
SELECT *, row_number() over (partition by TM_ID order by QUOTESYMBOL ASC) as seq
FROM TMP) TMP 
WHERE seq=1"""

conn.cursor().execute("""USE WAREHOUSE TM_DI_WH""")
data = conn.cursor().execute(query).fetch_pandas_all()
input_df = data[["PAIRS", "TM_ID"]]
pairs = list(input_df["PAIRS"])
dic = dict(zip(input_df.PAIRS, input_df.TM_ID))

# producer = KafkaProducer(
#     security_protocol='SASL_SSL',
#     sasl_mechanism = 'AWS_MSK_IAM',
#     api_version=(0,11,5),
#     bootstrap_servers=[
#         "b-1.tmccxtdata.022mgq.c5.kafka.us-east-1.amazonaws.com:9098",
#         "b-3.tmccxtdata.022mgq.c5.kafka.us-east-1.amazonaws.com:9098",
#         "b-2.tmccxtdata.022mgq.c5.kafka.us-east-1.amazonaws.com:9098"
#     ]
# )

async def loop(exchange, symbol):
    while True:
        try:
            orderbook = await exchange.watch_ticker(symbol)
            # print(orderbook)
            # print(exchange.iso8601(exchange.milliseconds()),"symbol:", orderbook['symbol'],"date::",orderbook['datetime'],"bid_price:",orderbook['bid'],"ask_price:",orderbook['ask'])
            res = {
                "timestamp": orderbook["timestamp"],
                "pairs": orderbook["symbol"],
                "bid_price": orderbook["bid"],
                "ask_price": orderbook["ask"],
               # "tm_id": dic[orderbook["symbol"]]
            }
            print(res)
            # producer.send('ccxt_binanceus_watchticker',json.dumps(res).encode('utf-8'))
            #time.sleep(1)
        except Exception as e:
            print(str(e))

async def main():
    exchange = ccxtpro.binancecoinm({'enableRateLimit': True})
    symbols = pairs
    #print(len(symbols))
    await asyncio.gather(*[loop(exchange, symbol) for symbol in symbols])
    await exchange.close()

if __name__ == '__main__':
    asyncio.run(main())
