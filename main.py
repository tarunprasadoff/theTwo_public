import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import asyncio
import os
from random import sample
import sys
import requests

from auth import Auth
from bal import getBalDict, getBal
from unique import unique
from sim import sim, fullSim
from quote import groupQuotes
from isInSymbsAndMarks import isInSymbsAndMarks
from order import executeCompOrder, executeOrder

auth = Auth()

endPoints = {"ordStat":"https://api.coindcx.com/exchange/v1/orders/status",
             "balance":"https://api.coindcx.com/exchange/v1/users/balances",
             "order":"https://api.coindcx.com/exchange/v1/orders/create",
             "ticker":"https://api.coindcx.com/exchange/ticker",
             "market_details":"https://api.coindcx.com/exchange/v1/markets_details",
             "markets":"https://api.coindcx.com/exchange/v1/markets",
             "ordBook":"https://public.coindcx.com/market_data/orderbook?pair=",
             "cancel":"https://api.coindcx.com/exchange/v1/orders/cancel",
             "ordEdit":"https://api.coindcx.com/exchange/v1/orders/edit"
            }

req = requests.get(endPoints["market_details"])
if not (req.status_code==200):
    print(req)
    print("Market Details Error")
    sys.exit()
da = req.json()
dadf = pd.DataFrame(da)
cryptos = [d['target_currency_short_name'] for d in da]
actexcs = ['USDT','INR','ETH','BTC']
symbs = [d['coindcx_name'] for d in da]
ecodes = {d['coindcx_name']:d['ecode'] for d in da}

time.sleep(1)

marksReq = requests.get(endPoints["markets"])
if not (marksReq.status_code==200):
    print(marksReq)
    print("Markets Error")
    sys.exit()
marks = marksReq.json()

pairIter = {}
for c in cryptos:
    for a in actexcs:
        if not (a == c):
            if isInSymbsAndMarks((c+a),symbs,marks):               
                pairIter[c+a] = (c,a)

actualPairs = []
for first in actexcs:
    for third in actexcs:
        if not (first == third):
            for second in cryptos:
                if second not in [first,third]:
                    if isInSymbsAndMarks((second+first),symbs,marks) and isInSymbsAndMarks((second+third),symbs,marks) and ( isInSymbsAndMarks((first+third),symbs,marks) or isInSymbsAndMarks((third+first),symbs,marks) ):
                        actualPairs.append((first,second,third))

actualPairs = unique(actualPairs)

time.sleep(1)

balDict = getBalDict(endPoints["balance"], auth)

time.sleep(1)
resName = "res.csv"
if not os.path.exists(resName):
    pd.DataFrame(columns=["pair","result","quote","actOrders","calcOrders","sig","time","bal"]).to_csv(resName,index=False)
tf = pd.read_csv(resName)

failName = "fail.csv"
if not os.path.exists(failName):
    pd.DataFrame(columns=["pair","quote","actOrders","calcOrders","sig","time","bal"]).to_csv(failName,index=False)
lf = pd.read_csv(failName)
lf.pair = lf.pair.apply(lambda x: eval(x))
lf.time = lf.time.apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f'))

blName = "bl.txt"
bl = ['FIL','YFI','TFUEL','SHIB','WNXM']

with open(blName, 'r') as filehandle:
    for line in filehandle:
        currentPlace = line[:-1]
        bl.append(currentPlace)

bl = unique(bl)

while True:
    
    print(datetime.now())
    
    try:
        final = sim(pairIter, marks, actualPairs, ecodes, endPoints['ticker'])
    except Exception as e:
        print("Initial Sim Error: ", e)
        time.sleep(1)
        continue
    
    pairs = unique([f[0] for f in final])
    
    pairs = [p for p in pairs if p[1] not in bl]
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        fq = loop.run_until_complete(groupQuotes(marks, pairs, ecodes, endPoints["ordBook"]))
        # fq = await groupQuotes(marks, pairs, ecodes, endPoints["ordBook"])
    except Exception as e:
        print("Full Quotes Fetch Error: ", e)
        time.sleep(1)
        continue
    
    tim = time.time()
    
    resuList = [fullSim(pairs[i][0],pairs[i][1],pairs[i][2],fq,getBal(balDict,pairs[i][0]),dadf,marks) for i in range(len(pairs))]
    
    sigIndList = [i for i in range(len(pairs)) if not (resuList[i] == None)]
    
    if len(sigIndList) > 0:
        
        sigInd = sample(sigIndList,1)[0]
        
        currSigInd = pairs[sigInd]

        first, second, third = currSigInd[0], currSigInd[1], currSigInd[2]
        
        resu = resuList[sigInd]
        
        print("Time taken for Computing Orders", time.time() - tim)
        
        if not (resu == None):

            print(f"Arbitrage Spotted: {first}-{second}-{third}")
            print(resu)
            orders = resu[1]
            sigData = resu[0]
            print(orders)

            ord1Data = []
            ord2Data = []
            ord3Data = []
            
            a1, a2, a3 = orders[0][2], orders[1][2], orders[2][2]
            f1, f2, f3 = 0, 0, 0
            
            isMarketCalled = False
            
            ord1s, ord1Stats, ord1Cancs, ord1Res, c1 = executeOrder(orders[0][0],orders[0][1],orders[0][2],orders[0][3],1,dadf,endPoints,auth)
            ord1Data.append([ord1s, ord1Stats, ord1Cancs, ord1Res, None])
            f1 += c1

            if f1 == 0:
                print("Signal not Executed")
                
                pairData = (first,second,third)
                actOrderData = [ord1Data]
                appendData = {"pair":pairData,"quote":fq,"actOrders":actOrderData,"calcOrders":orders,"sig":sigData,"time":datetime.now(),"bal":balDict}
                lf = lf.append(appendData,ignore_index=True)
                lf.to_csv(failName, index=False)
                
                tenthFailDf = lf[lf.pair.apply(lambda x: x[1]==second)][-10:]
                if len(tenthFailDf)==10:
                    tenthTime = tenthFailDf.time.values[0]
                    if not (type(tenthTime) == datetime):
                        if type(tenthTime) == np.datetime64:
                            tenthTime = pd.Timestamp(tenthTime).to_pydatetime()
                        else:
                            print("tenthTime type error", tenthTime)
                            sys.exit()
                    if (datetime.now() - timedelta(hours=1)) <= tenthTime:
                        bl.append(second)
                        bl = unique(bl)
                        with open(blName, 'w') as filehandle:
                            for listitem in bl:
                                filehandle.write('%s\n' % listitem)
                
                continue
            else:
                while (f1<a1):
                    isMarketCalled = True
                    ord1s, ord1Stats, ord1Cancs, ord1Res, ord1Quotes, c1 = executeCompOrder(orders[0][0],orders[0][1],(a1-f1),orders[0][3],1, isMarketCalled, dadf, endPoints, ecodes, auth)
                    ord1Data.append([ord1s, ord1Stats, ord1Cancs, ord1Res, ord1Quotes])
                    f1 += c1
                    if ord1Res == 200:
                        orders[1][2] -= c1
                        a2 -= c1
            
            ord2s, ord2Stats, ord2Cancs, ord2Res, ord2Quotes, c2 = executeCompOrder(orders[1][0],orders[1][1],(a2-f2),orders[1][3],2, isMarketCalled, dadf, endPoints, ecodes, auth)
            ord2Data.append([ord2s, ord2Stats, ord2Cancs, ord2Res, ord2Quotes])
            f2 += c2
            
            while (f2<a2):
                isMarketCalled = True
                ord2s, ord2Stats, ord2Cancs, ord2Res, ord2Quotes, c2 = executeCompOrder(orders[1][0],orders[1][1],(a2-f2),orders[1][3],2, isMarketCalled, dadf, endPoints, ecodes, auth)
                ord2Data.append([ord2s, ord2Stats, ord2Cancs, ord2Res, ord2Quotes])
                f2 += c2

            ord3s, ord3Stats, ord3Cancs, ord3Res, ord3Quotes, c3 = executeCompOrder(orders[2][0],orders[2][1],(a3-f3),orders[2][3],3, isMarketCalled, dadf, endPoints, ecodes, auth)
            ord3Data.append([ord3s, ord3Stats, ord3Cancs, ord3Res, ord3Quotes])
            f3 += c3
                
            while (f3<a3):
                isMarketCalled = True
                ord3s, ord3Stats, ord3Cancs, ord3Res, ord3Quotes, c3 = executeCompOrder(orders[2][0],orders[2][1],(a3-f3),orders[2][3],3, isMarketCalled, dadf, endPoints, ecodes, auth)
                ord3Data.append([ord3s, ord3Stats, ord3Cancs, ord3Res, ord3Quotes])
                f3 += c3

            print(f"Arbitrage Completed: {first}-{second}-{third}")
            
            oldBalDict = {a:getBal(balDict,a) for a in actexcs}
            
            print("Calculating new Balances")
            
            time.sleep(.2)
            
            balDict = getBalDict(endPoints["balance"], auth)
            
            balanceDiffData = {a:(getBal(balDict,a)-oldBalDict[a]) for a in actexcs}
            print(balanceDiffData)
                
            pairData = (first,second,third)
            actOrderData = [ord1Data,ord2Data,ord3Data]
            appendData = {"pair":pairData,"result":balanceDiffData,"quote":fq,"actOrders":actOrderData,"calcOrders":orders,"sig":sigData,"time":datetime.now(),"bal":balDict}
            tf = tf.append(appendData,ignore_index=True)
            tf.to_csv(resName, index=False)