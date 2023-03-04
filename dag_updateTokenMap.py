from distutils.log import error
import json
import logging
from multiprocessing import set_forkserver_preload
import os
from webbrowser import get
from colorama import Cursor
from pytz import timezone
from sqlalchemy import true
import urllib3
import re
from re import I
import time
import pickle
import requests
import sys
import hashlib 
import traceback
from datetime import datetime, timedelta
import pandas as pd
from requests.api import head
import snowflake.connector
import snowflake.connector as connector
import snowflake.connector.pandas_tools as pdt
# from airflow.exceptions import AirflowException
# from airflow.models import DAG, Variable
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy.sql import schema

import pandas as pd
import ccxt.pro as ccxtpro
from asyncio import run
from kafka import KafkaProducer
import asyncio
import ccxt
import boto3
from collections import defaultdict
import multiprocessing
from multiprocessing import Process
import threading
from threading import Thread
import random


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logger = logging.getLogger()
logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(filename)s] %(message)s', level=logging.INFO)
logFormatter = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s] %(message)s')
logger.handlers[0].setFormatter(logFormatter)

#for conn
WAREHOUSE = "TM_DI_WH"
ROLE = "TM_DATAINTEGRATION"
SCHEMA = "COINGECKO"
DATABASE = "CRYPTO_DB"
USER=Variable.get("TM_CORE_USR")
ACCOUNT=Variable.get("SF_ACCT")

#for conn1
USER1=Variable.get("TM_CORE_USR")
ACCOUNT1=Variable.get("SF_ACCT")
PASSWORD= 'Token@1234'
DATABASE1 = 'DEMO_DB'
SCHEMA1 = 'PUBLIC'
ROLE1='TM_INTERNAL_TEAM'
WAREHOUSE1 = 'TM_INTERNAL_WH'
conn1=getConnection(ACCOUNT1,WAREHOUSE1,DATABASE1,ROLE1,SCHEMA1,USER1)


def getConnection(ACCOUNT,WAREHOUSE,DATABASE,ROLE,SCHEMA,USER):
    logger.info("Connection Parameters : ")
    logger.info("ACCOUNT : "+ACCOUNT)
    logger.info("WAREHOUSE : "+WAREHOUSE)
    logger.info("DATABASE : "+DATABASE)
    logger.info("ROLE : "+ROLE)
    logger.info("SCHEMA : "+SCHEMA)
    logger.info("USER : "+USER)
    pemFile =  os.path.join(os.path.expanduser('~') ,".pem",str(USER).lower()+".pem")
    with open(pemFile, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
     # SF Connection 
    ctx = snowflake.connector.connect(
        user=str(USER),
        account=ACCOUNT,
        private_key=pkb,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        ROLE=ROLE
        )
    return ctx


conn = getConnection(ACCOUNT,WAREHOUSE,DATABASE,ROLE,SCHEMA,USER)


def unmatched_symbol(symbols,item):
    exchange_name=item
    symbols_to_be_removed=[]
    symbols_to_be_added=[]
    logger.info("................exchangewise separates out tokens not mapped to tokens from CCXT_TOKEN_MAP tokens for "+item +" exchange...............")
    query = """
    SELECT PAIRS, BASESYMBOL, QUOTESYMBOL, TM_ID FROM CRYPTO_DB.CCXT_PRO.CCXT_TOKEN_MAP
    WHERE EXCHANGE='{exchange}'""".format(exchange=exchange_name)
    cur = conn.cursor()
    data = cur.execute(query).fetch_pandas_all()
    data=data[~data['PAIRS'].str.contains(':')]
    input_df = data[["PAIRS", "TM_ID"]]
    pairs = list(input_df["PAIRS"])
    print(len(pairs))
    for pair in pairs:
        if pair not in symbols:
            symbols_to_be_removed.append(pair)
    for symbol in symbols:
        if symbol not in pairs:
            symbols_to_be_added.append(symbol)
    print("symbols to be removed............")
    #print(symbols_to_be_removed)
    return symbols_to_be_removed, symbols_to_be_added


async def get_token_pairs(item):
    exchange=getattr(ccxtpro,item)
    exchange=exchange()
    markets = await exchange.load_markets()
    pairs = pd.Series(list(markets.keys()))

    baseAssets = [i.split("/")[0] for i in pairs]
    quoteAssets = [i.split("/")[1] for i in pairs]
    df = pd.DataFrame({
        "PAIRS": pairs,
        "EXCHANGE": [exchange.id] * len(pairs),
        "BASESYMBOL": baseAssets,
        "QUOTESYMBOL": quoteAssets
    })
    await exchange.close()
    return df

def get_tm_cg_id(conn,l):
    if len(l)>1:
        l=tuple(l)
        query1= """SELECT ID, NAME, SYMBOL, CG_ID FROM CRYPTO_DB.COINGECKO.COINGECKO_TOKENS WHERE STATUS='ACTIVE' and SYMBOL in {}""".format(l)
    elif len(l)==1:
        token=l[0]
        query1= """SELECT ID, NAME, SYMBOL, CG_ID FROM CRYPTO_DB.COINGECKO.COINGECKO_TOKENS WHERE STATUS='ACTIVE' and SYMBOL ='{token}'"""
    data = conn.cursor().execute(query1).fetch_pandas_all()
    data["SYMBOL"] = data["SYMBOL"].str.upper()
    return data 

def merged_paris_with_cg_id(pairs_df, cg_id_df):
    logger.info("..........MERGES TOKEN DETAILS FROM LOAD MARKET AND COINGECKO_TOKENS TABLE..........")
    merged_df = pairs_df.merge(cg_id_df, left_on="BASESYMBOL", right_on="SYMBOL", how="inner")
    merged_df["ID"] = merged_df["ID"].astype("int")
    return merged_df

async def get_latest_price_from_ccxt(pair,item):
    data1={}
    if item != 'mexc':
        exchange=getattr(ccxtpro, item)
        exchange=exchange()
        logger.info("supports watchticker")
        data1=await (exchange.watch_ticker(pair))
        await exchange.close()
    else:
        logger.info("supports fetchticker")
        exchange=getattr(ccxt,item)
        exchange=exchange()
        data1=exchange.fetch_ticker(pair)
    res = {"bidkey": data1["bid"]}
    v=res['bidkey']
    #print("bid",v)
    if v is None:
        return 0
    else:
        return float(v)

def get_latest_price_from_sf(id, conn):
    logger.info(".............fetching price for token from ccxt........")
    query = f"""SELECT * FROM CRYPTO_DB.COINGECKO.COINGECKO_CURR_LATEST_TOKEN_PRICE_VIEW WHERE TOKEN_ID={id} ORDER BY ID DESC LIMIT 1"""
    data = conn.cursor().execute(query).fetch_pandas_all()
    return float(data["CURRENT_PRICE"][0])

def compare_two_price(ccxt_price, sf_price):
    if ccxt_price == 0:
        return 1
    diff = abs(ccxt_price - sf_price) / ccxt_price
    return diff

async def choose_tm_id(df, baseAsset,item):
    # df has three columns, PAIRS, BASESYMBOL, ID
    
    # unique id
    if 1 == len(df["ID"].unique()):
        return {baseAsset: list(df["ID"].unique())[0]}
       # more than one id 
    df = df[["PAIRS", "BASESYMBOL", "ID"]]
    for index, row in df.iterrows():
        pair = row[0]
        baseSymbol = row[1]
        token_id = row[2]
        try:
            ccxt_price =await get_latest_price_from_ccxt(pair, item)
            sf_price = get_latest_price_from_sf(token_id, conn)
            diff = compare_two_price(ccxt_price, sf_price)
            if diff <= 0.01:
                return {baseAsset: token_id}
            return {baseAsset:False}
        except Exception as e:
            print(e)
            return{baseAsset:False}
        
        
def load_table(conn4,res,item):
    logger.info(".........LOADING NEW TOKEN DETAILS TO CCXT_TOKEN_MAP FROM EXCHANGE  "+item+ " .........")
    write_pandas(conn4, df=res, table_name="CCXT_TOKEN_MAP", database="CRYPTO_DB", schema="CCXT_PRO")


def update_status(conn4,l,item):
    logger.info("..........UPDATING STATUS OF TOKENS REMOVED FROM EXCHANGE  "+item+".........")
    token=l[0]
    if len(l)>1:
        query_update_status=f"""update CRYPTO_DB.CCXT_PRO.CCXT_TOKEN_MAP set STATUS = FALSE where EXCHANGE='{item}' and PAIRS in {tuple(l)}"""
    else:
        query_update_status=f"""update CRYPTO_DB.CCXT_PRO.CCXT_TOKEN_MAP set STATUS = FALSE where EXCHANGE='{item}' and PAIRS = '{token}'"""
    cur = conn.cursor()
    cur.execute(query_update_status)
    return

def thread_handle(item,conn):
    symbols_df1=asyncio.run(get_token_pairs(item))

    symbols_df=symbols_df1[~symbols_df1['PAIRS'].str.contains(':')]
    symbols=list(symbols_df['PAIRS'])
           
    symbols_to_be_removed,symbols_to_be_added=unmatched_symbol(symbols,item)
    print("length of symbols_to_be_removed",len(symbols_to_be_removed))

    l=[i.split("/")[0] for i in symbols_to_be_added]
    tmp=symbols_df.query('PAIRS in @symbols_to_be_added')
    n=[ a.lower() for a in l ]
    m=[ a.upper() for a in l ]
    l=n+m

    try:
        if len(symbols_to_be_removed)>0:
            update_status(conn,symbols_to_be_removed,item)
        if len(symbols_to_be_added)>0:
            tm_cg_id_df = get_tm_cg_id(conn,l)
            merged_df = merged_paris_with_cg_id(tmp, tm_cg_id_df)
            unique_basesymbol = merged_df["BASESYMBOL"].unique()
   
            output = []
            for base in unique_basesymbol:
                tmp_df = merged_df[merged_df["BASESYMBOL"] == base]
                res= asyncio.run(choose_tm_id(tmp_df, base,item))
                output.append(res)
            unmatched_price_token=[]

            dic = defaultdict(int)
            for i in output:
                key = list(i.keys())[0]
                value =  i[key]
                if value == False:
                    query_o = "insert into DEMO_DB.PUBLIC.unmatched_price_token (KEY, EXCHANGE) values " +f"('{key}','{item}')"
                    data = conn1.cursor().execute(query_o)
                    unmatched_price_token.append([key,item])
                    value = pd.NA
                dic[key] = value
                dic["ETH"] = 3306

            unique_symbols = list(tmp["BASESYMBOL"].unique())

            for symbol in unique_symbols:
                if symbol not in dic:
                    dic[symbol] = pd.NA
            
            tm_ids = []
            for sym in list(tmp["BASESYMBOL"]):
                tm_ids.append(dic[sym])

          
            tmp=tmp.assign(TM_ID=tm_ids)
            #print("tmp1")
            #print(tmp)

            res = tmp.merge(tm_cg_id_df, left_on="TM_ID", right_on="ID", how="left")
            res = res[["PAIRS", "EXCHANGE", "BASESYMBOL", "QUOTESYMBOL", "TM_ID", "NAME", "CG_ID"]]
            #print(res)
        #load_table(conn, res)

        print("..........DONE with  "+item+ "...........")
    except Exception as e:
        print(e)
    
    

def threadinit():
    #exchangelist=['binanceus','bitfinex2','bitget','bitmart','bybit','coinbasepro','coinex','cryptocom','coinex','cryptocom','gateio','huobi','kraken','kucoin','mexc','okx','upbit','whitebit']
    exchangelist=['bitmart']
    threads=[]
    for item in exchangelist:
        th =Thread(target=thread_handle, args = (item,conn))
        threads.append(th)
        th.start()


default_args = {
    'start_date': datetime.datetime(2023, 3, 3),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
        dag_id ='update_ccxt_token_map_table',
         schedule_interval='0 0 * * *',
         default_args=default_args,
         catchup=False,
         dagrun_timeout=timedelta(minutes=30),
         tags=["database=crypto_db","schema=coingecko","@1 hr","@daily","ccxt_token_map"])

ccxt_exchangeToken_update = PythonOperator(
    task_id='ld_ccxt_exchange_tokens_hourly',
    python_callable=threadinit,
    dag=dag
)


ccxt_exchangeToken_update