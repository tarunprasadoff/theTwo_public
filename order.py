import time

from truncate import truncate
from auth_req import makeAuthReq

import requests

def getDepthSync(crypto, base, endPoints, ecodes):
    try:
        pairSymb = ecodes[(crypto+base)] + '-' + crypto + '_' + base
        req = requests.get(endPoints["ordBook"] + pairSymb)
        reqJson = req.json()
        if not (req.status_code==200):
            print(pairSymb,"Request Status Error", req, reqJson)
            reqJson = {'bids':None,'asks':None}
    except Exception as l:
        print(pairSymb,"Request Json Error", l)
        reqJson = {'bids':None,'asks':None}
        
    if reqJson == None:
        reqJson = {'bids':None,'asks':None}
    
    if not (reqJson['bids']==None):
        if len(reqJson['bids']) == 0:
            reqJson['bids'] = None
        else:
            quoteDict = {float(rj):rj for rj in list(reqJson['bids'].keys())}
            quotes = list(quoteDict.keys())
            quotes.sort(reverse=True)
            reqJson['bids'] = [{'rate':q,'btc':float(reqJson['bids'][quoteDict[q]])} for q in quotes]
    
    if not (reqJson['asks']==None):
        if len(reqJson['asks']) == 0:
            reqJson['asks'] = None
        else:
            quoteDict = {float(rj):rj for rj in list(reqJson['asks'].keys())}
            quotes = list(quoteDict.keys())
            quotes.sort()
            reqJson['asks'] = [{'rate':q,'btc':float(reqJson['asks'][quoteDict[q]])} for q in quotes]
        
    return {'buy':reqJson['bids'],'sell':reqJson['asks']}

def getCurrentPrice(symbol, side, quantity, rate, ordNo, ordQuotes, dadf, endPoints, ecodes):
    
    while True:
        
        symbolDf = dadf[dadf["coindcx_name"]==symbol]
        targetCurr = symbolDf.target_currency_short_name.values[0]
        baseCurr = symbolDf.base_currency_short_name.values[0]
        fullOrdQuote = getDepthSync(targetCurr,baseCurr,endPoints,ecodes)

        if side == 'buy':
            ordQuote = fullOrdQuote['sell']
        elif side == 'sell':
            ordQuote = fullOrdQuote['buy']
            
        ordQuotes.append(ordQuote)

        if not (ordQuote == None):
            
            currPrice = None
            
            if len(ordQuote)>0:
                if ordQuote[0]['btc'] >= quantity:
                    currPrice = ordQuote[0]['rate']
                # for exiting immediately
                elif ((ordQuote[0]['rate'] >= (0.98 * rate)) and (side == 'SELL')) or ((ordQuote[0]['rate'] <= (1.02 * rate)) and (side == 'BUY')):
                    print("Initiating Subpar Exit")
                    currPrice = ordQuote[0]['rate']
            else:
                print("No Quote")
                    
            return currPrice, ordQuotes
                
        else:
            print(f"Order {(symbol, side, quantity, rate, ordNo)} Quote Error")
            time.sleep(.5)

def myOrderStatus(ordId,endPoints,auth):
    while True:
        ordStatReq = makeAuthReq(endPoints["ordStat"],{"id":ordId},auth)
        if not (ordStatReq.status_code==200):
            print("Order Status Request Error")
            print(ordStatReq)
            try:
                ordStat = ordStatReq.json()
                print(ordStat)
            except Exception as e:
                print("Order Status Json Error", e)
            time.sleep(.5)
        else:
            print(ordStatReq)
            ordStat = ordStatReq.json()
            return ordStat

def myCancelOrder(ordId,endPoints,auth):
    while True:
        ordCancReq = makeAuthReq(endPoints["cancel"],{"id":ordId},auth)
        if not (ordCancReq.status_code==200):
            print("Order Cancel Request Error")
            print(ordCancReq)
            try:
                ordCanc = ordCancReq.json()
                print(ordCanc)
            except Exception as e:
                print("Order Cancel Json Error", e)
            return None
        else:
            print(ordCancReq)
            ordCanc = ordCancReq.json()
            return ordCanc

def executeOrder(symbol, side, quantity, rate, ordNo, dadf, endPoints, auth):

    ords = []
    ordStats = []
    ordCancs = []
    
    t = .1
    ot = .5

    quantity =  truncate(quantity, int(dadf[dadf['coindcx_name']==symbol].target_currency_precision.values[0]))
    
    ordiReq = makeAuthReq(endPoints["order"],{"order_type": "limit_order", "market": symbol, "side": side, "total_quantity": quantity, "price_per_unit": rate,},auth)
    print(ordiReq)
    try:
        ordi = ordiReq.json()
        print(ordi)
    except Exception as e:
        print("Order Json Error", e)
    if not (ordiReq.status_code==200):
        print(f"Order {(symbol, side, quantity, rate, ordNo)} Failed")
        return ords, ordStats, ordCancs, -1, 0
    ords.append(ordi)
    print(f"Order {(symbol, side, quantity, rate, ordNo)} Placed")
    
    ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
    ordStats.append(ordStat)
    print(ordStat)
    
    if ordStat['status'] == "rejected":
        print(f"Order {(symbol, side, quantity, rate, ordNo)} Rejected")
        return ords, ordStats, ordCancs, -1, 0
    
    time.sleep(t)
    
    if ordStat['status'] == "filled":
        print(f"Order {ordNo} Executed")
        return ords, ordStats, ordCancs, 20, (ordStat['total_quantity']-ordStat['remaining_quantity'])
    else:
        time.sleep(ot)
        ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
        ordStats.append(ordStat)
        print(ordStat)
        
        if ordStat['status'] == "filled":
            print(f"Order {ordNo} Executed")
            return ords, ordStats, ordCancs, 20, (ordStat['total_quantity']-ordStat['remaining_quantity'])
        else:
            print(f"Entering Cancelling Loop for Order {(symbol, side, quantity, rate, ordNo)}")
            
            while True:
                ordCanc = myCancelOrder(ordi["orders"][0]['id'],endPoints,auth)
                ordCancs.append(ordCanc)
                print(ordCanc)
                
#                 time.sleep(ot)
                ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
                ordStats.append(ordStat)
                print(ordStat)
                
                if (ordStat['status'] == 'partially_cancelled') or (ordStat['status'] == 'cancelled'):
                    print(f"Order {ordNo} Canceled")
                    return ords, ordStats, ordCancs, -1, (ordStat['total_quantity']-ordStat['remaining_quantity'])
                elif (ordStat['status'] == 'filled'):
                    print(f"Order {ordNo} Executed")
                    return ords, ordStats, ordCancs, 2, (ordStat['total_quantity']-ordStat['remaining_quantity'])
                elif ordStat['status'] == "rejected":
                    print(f"Order {(symbol, side, quantity, rate, ordNo)} Rejected")
                    return ords, ordStats, ordCancs, -1, 0

def executeCompOrder(symbol, side, quantity, rate, ordNo, isMarketCalled, dadf, endPoints, ecodes, auth):
    ords = []
    ordStats = []
    ordCancs = []
    ordQuotes = []
    marketOrderSleepMult = 1
    
    t = .1
    ot = .5
    
    if isMarketCalled:
        currPrice, ordQuotes = getCurrentPrice(symbol, side, quantity, rate, ordNo, ordQuotes, dadf, endPoints, ecodes)
        if currPrice == None:
            currPrice = rate
            marketOrderSleepMult = 4
    else:
        currPrice = rate

    if ( (currPrice*quantity) < dadf[dadf['coindcx_name']==symbol].min_notional.values[0] ):
        print("Order Below Min Notional")
        return ords, ordStats, ordCancs, 200, ordQuotes, quantity

    if ( quantity < (10**(-int(dadf[dadf['coindcx_name']==symbol].target_currency_precision.values[0]))) ):
        print("Order Below Target Precision")
        return ords, ordStats, ordCancs, 200, ordQuotes, quantity

    quantity =  truncate(quantity, int(dadf[dadf['coindcx_name']==symbol].target_currency_precision.values[0]))

    ordiReq = makeAuthReq(endPoints["order"],{"order_type": "limit_order", "market": symbol, "side": side, "total_quantity": quantity, "price_per_unit": currPrice,},auth)
    print(ordiReq)
    try:
        ordi = ordiReq.json()
        print(ordi)
    except Exception as e:
        print("Order Json Error", e)
    if not (ordiReq.status_code==200):
        
        try:
            if (ordi['message'] == 'Quantity too low'):
                return ords, ordStats, ordCancs, 200, ordQuotes, quantity
        except Exception as e:
            print("Second Order Json Error", e)
        
        print(f"Order {(symbol, side, quantity, rate, ordNo)} Failed")
        return ords, ordStats, ordCancs, -1, ordQuotes, 0
    ords.append(ordi)
    print(f"Order {(symbol, side, quantity, rate, ordNo)} Placed")
    
    ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
    ordStats.append(ordStat)
    print(ordStat)
    
    if ordStat['status'] == "rejected":
        print(f"Order {(symbol, side, quantity, rate, ordNo)} Rejected")
        return ords, ordStats, ordCancs, -1, ordQuotes, 0
    
    time.sleep(t*marketOrderSleepMult)
    
    if ordStat['status'] == "filled":
        print(f"Order {ordNo} Executed")
        return ords, ordStats, ordCancs, 20, ordQuotes, (ordStat['total_quantity']-ordStat['remaining_quantity'])
    else:
        
        while True:
            
            time.sleep(ot*marketOrderSleepMult)
            ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
            ordStats.append(ordStat)
            print(ordStat)

            if ordStat['status'] == "filled":
                print(f"Order {ordNo} Executed")
                return ords, ordStats, ordCancs, 20, ordQuotes, (ordStat['total_quantity']-ordStat['remaining_quantity'])
            elif ordStat['status'] == "rejected":
                print(f"Order {(symbol, side, quantity, rate, ordNo)} Rejected")
                return ords, ordStats, ordCancs, -1, ordQuotes, 0
            else:
                currPrice, ordQuotes = getCurrentPrice(symbol, side, quantity, rate, ordNo, ordQuotes, dadf, endPoints, ecodes)
                if not (currPrice == None):
                    print(f"Order {(symbol, side, quantity, rate, ordNo)} Editing to {currPrice}")
                    ordE = makeAuthReq(endPoints["ordEdit"],{"id":ordi["orders"][0]['id'],"price_per_unit": currPrice},auth)

                    try:
                        print(ordE.json())
                    except Exception as e:
                        print("Order Edit Json Error", e)
                    
                    if ordE.status_code == 422:
                        if (ordE.json()['message'] == 'Cannot edit this order') or (ordE.json()['message'] == 'Edit order is not available yet for this exchange') or (ordE.json()['message'] == 'Unable to edit this order on Matching Engine'):
                            while True:
                                print("Cancelling as cant edit")
                                ordCanc = myCancelOrder(ordi["orders"][0]['id'],endPoints,auth)
                                ordCancs.append(ordCanc)
                                print(ordCanc)

                                ordStat = myOrderStatus(ordi["orders"][0]['id'],endPoints,auth)
                                ordStats.append(ordStat)
                                print(ordStat)
                                
                                if (ordStat['status'] == 'partially_cancelled') or (ordStat['status'] == 'cancelled'):
                                    print(f"Order {ordNo} Canceled")
                                    return ords, ordStats, ordCancs, -1, ordQuotes, (ordStat['total_quantity']-ordStat['remaining_quantity'])
                                elif (ordStat['status'] == 'filled'):
                                    print(f"Order {ordNo} Executed")
                                    return ords, ordStats, ordCancs, 2, ordQuotes, (ordStat['total_quantity']-ordStat['remaining_quantity'])
                                elif ordStat['status'] == "rejected":
                                    print(f"Order {(symbol, side, quantity, rate, ordNo)} Rejected")
                                    return ords, ordStats, ordCancs, -1, ordQuotes, 0

                    marketOrderSleepMult = 1.5