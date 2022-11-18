from requests import get
import pandas as pd
from truncate import truncate

def pdSim(first, second, third, currQuotes, feeFactors, final, symbs):

    if ((first in currQuotes[second].keys()) and (third in currQuotes[second].keys())):

        feeFactor = feeFactors[second][first]
        start = currQuotes[second][first]["sell"]
        if start == None:
            return
        start = float(start)
        mid1 = feeFactor

        feeFactor = feeFactors[second][third]
        mid2Quote = currQuotes[second][third]["buy"]
        if mid2Quote == None:
            return
        mid2Quote = float(mid2Quote)
        if (start == 0) or (mid2Quote == 0):
            return

        mid2 = (mid1*mid2Quote)*feeFactor

        if ((first+third) in symbs):
            feeFactor = feeFactors[first][third]
            endQuote = currQuotes[first][third]["sell"]
            if endQuote == None:
                return
            endQuote = float(endQuote)
            if (endQuote == 0):
                return
            end = (mid2/endQuote)*feeFactor
            if (end>start):
                final.append(((first,second,third),(end/start)))
        elif ((third+first) in symbs):
            feeFactor = feeFactors[third][first]
            endQuote = currQuotes[third][first]["buy"]
            if endQuote == None:
                return
            endQuote = float(endQuote)
            if (endQuote == 0):
                return
            end = (mid2*endQuote)*feeFactor
            if (end>start):
                final.append(((first,second,third),(end/start)))

def pdGetSimQuoteAndFee(pairIter, market, bids, asks, marks, currQuotes, feeFactors, symbs, ecodes):

    if not (market in marks):
        return

    try:
        c, a = pairIter[market]
    except:
        return

    if not (c in currQuotes.keys()):
        currQuotes[c] = {}
        feeFactors[c] = {}
    currQuote = {}
    currFeeFactor = {}
    try:
        bi = float(bids)
    except:
        bi = None
    try:
        si = float(asks)
    except:
        si = None
    currQuote[a] = {'buy':bi,'sell':si}
    currFeeFactor[a] = 1 - ( getFeesInPercent(ecodes[(c+a)]) / 100 )
    currQuotes[c].update(currQuote)
    feeFactors[c].update(currFeeFactor)
    symbs.append(c+a)

def sim(pairIter, marks, pairs, ecodes, endPoint):

    req = get(endPoint)
    df = pd.DataFrame(req.json())
    currQuotes = {}
    feeFactors = {}
    symbs = []
    df.apply(lambda x: pdGetSimQuoteAndFee(pairIter, x["market"], x["bid"], x["ask"], marks, currQuotes, feeFactors, symbs, ecodes), axis=1)
    final = []
    pd.DataFrame(pairs).apply(lambda x: pdSim(x[0],x[1],x[2], currQuotes, feeFactors, final, symbs), axis=1)   
    return final

def fullSim(first,second,third,fq,bal1,dadf,marks):

    bidFraction=0.75
    
    if fq == None:
        print(f"{first}-{second}-{third}: Empty Total Quote")
        return None
    
    if bal1 <= 0:
        print(f"{first}-{second}-{third}: Empty Balance")
        return None
    
    fq1 = fq[second][first]["sell"]
    fq2 = fq[second][third]["buy"]
    
    feesInPercent1 = getFeesInPercent(dadf[dadf['coindcx_name']==(second+first)].ecode.values[0])
    feesInPercent2 = getFeesInPercent(dadf[dadf['coindcx_name']==(second+third)].ecode.values[0])
    
    startCountMinLimit = dadf[dadf['coindcx_name']==(second+first)].min_notional.values[0]
    mid2CountMinLimit = dadf[dadf['coindcx_name']==(second+third)].min_notional.values[0]
    
    if (first+third) in marks:
        fq3 = fq[first][third]["sell"]
        feesInPercent3 = getFeesInPercent(dadf[dadf['coindcx_name']==(first+third)].ecode.values[0])
        mid2CountMinLimit = max(dadf[dadf['coindcx_name']==(first+third)].min_notional.values[0], mid2CountMinLimit)
    elif (third+first) in marks:
        fq3 = fq[third][first]["buy"]
        feesInPercent3 = getFeesInPercent(dadf[dadf['coindcx_name']==(third+first)].ecode.values[0])
        startCountMinLimit = max(dadf[dadf['coindcx_name']==(third+first)].min_notional.values[0],startCountMinLimit)
    
    if ((fq1 == None) or (fq2 == None) or (fq3 == None)):
        print(f"{first}-{second}-{third}: Empty Quote")
        return None
    
    startRate, mid1Count = fq1[0]['rate'], fq1[0]['btc']
    mid2Rate, mid1CountFrom2 = fq2[0]['rate'], fq2[0]['btc']
    
    if (first+third) in marks:
        mid2RateFrom3, endCount = fq3[0]['rate'], fq3[0]['btc']
    elif (third+first) in marks:
        endRate, mid2CountFrom3 = fq3[0]['rate'], fq3[0]['btc']
    
    startPrecision = int(dadf[dadf['coindcx_name']==(second+first)].base_currency_precision.values[0])
    mid1Precision = int(min(dadf[dadf['coindcx_name']==(second+first)].target_currency_precision.values[0],dadf[dadf['coindcx_name']==(second+third)].target_currency_precision.values[0]))
    mid2Precision = int(dadf[dadf['coindcx_name']==(second+third)].base_currency_precision.values[0])
    
    tempMid1Count = truncate((mid1Count*bidFraction), mid1Precision)
    tempStartCount = round((startRate*tempMid1Count*(1+feesInPercent1*.01)), startPrecision)
    if tempStartCount >= bal1:
        tempStartCount = (bal1*bidFraction) / (1+feesInPercent1*.01)
        tempMid1Count = truncate((tempStartCount / startRate), mid1Precision)
        tempStartCount = round( ( (tempMid1Count * startRate) * (1+feesInPercent1*.01) ), startPrecision)
        
    if ( ( 0 < tempStartCount <= bal1 ) and ( 0 < tempMid1Count <= mid1Count ) ):
        startCount, mid1Count = tempStartCount, tempMid1Count
    else:
        print(f"{first}-{second}-{third}: Order1 Error")
        return None

    if ( startCount <= startCountMinLimit ):
        print(f"{first}-{second}-{third}: {first} below Min Volume")
        return None
    
    tempMid1CountFrom2 = truncate((mid1CountFrom2*bidFraction), mid1Precision)
    if mid1Count <= tempMid1CountFrom2:
        tempMid2Count = truncate((mid2Rate*mid1Count*(1-feesInPercent2*.01)), mid2Precision)
    else:
        tempMid1Count = tempMid1CountFrom2
        tempStartCount = round((startRate*tempMid1Count*(1+feesInPercent1*.01)), startPrecision)
        tempMid2Count = truncate((mid2Rate*tempMid1Count*(1-feesInPercent2*.01)), mid2Precision)
    
    if ( ( 0 < tempStartCount <= bal1 ) and ( 0 < tempMid1Count <= mid1Count ) and ( tempMid2Count > 0 ) ):
        startCount, mid1Count, mid2Count = tempStartCount, tempMid1Count, tempMid2Count
    else:
        print(f"{first}-{second}-{third}: Order2 Error")
        return None
    
    if ( startCount <= startCountMinLimit ):
        print(f"{first}-{second}-{third}: {first} below Min Volume")
        return None
    elif ( mid2Count <= mid2CountMinLimit ):
        print(f"{first}-{second}-{third}: {third} below Min Volume")
        return None
    
    if (first+third) in marks:
        newStartPrecision = int(dadf[dadf['coindcx_name']==(first+third)].target_currency_precision.values[0])
        newMid2Precision = int(dadf[dadf['coindcx_name']==(first+third)].base_currency_precision.values[0])
    elif (third+first) in marks:
        newStartPrecision = int(dadf[dadf['coindcx_name']==(third+first)].base_currency_precision.values[0])
        newMid2Precision = int(dadf[dadf['coindcx_name']==(third+first)].target_currency_precision.values[0])

    if (third+first) in marks:
        
        tempMid2CountFrom3 = truncate((mid2CountFrom3*bidFraction), newMid2Precision)
        if mid2Count <= tempMid2CountFrom3:
            mid2Count = truncate(mid2Count,newMid2Precision)
            tempEndCount = truncate((endRate*mid2Count*(1-feesInPercent3*.01)), newStartPrecision)
        else:
            tempMid1Count = round((tempMid2CountFrom3 / ((1-feesInPercent2*.01)*mid2Rate)), mid1Precision)
            tempStartCount =  round((startRate*tempMid1Count*(1+feesInPercent1*.01)), startPrecision)
            tempMid2Count = truncate((mid2Rate*tempMid1Count*(1-feesInPercent2*.01)), min(newMid2Precision,mid2Precision))
            tempEndCount = truncate(((tempMid2Count*endRate)*(1-feesInPercent3*.01)), newStartPrecision)
            

    elif (first+third) in marks:
        
        tempEndCount = mid2Count/mid2RateFrom3
        tempEndCountLimit = truncate((endCount*bidFraction),newStartPrecision)
        if tempEndCount <= tempEndCountLimit:
            mid2Count = truncate(mid2Count,newMid2Precision)
            tempEndCount = truncate((tempEndCount*(1-feesInPercent3*.01)), newStartPrecision)
        else:
            tempMid1Count = round(((tempEndCountLimit * mid2RateFrom3) / ((1-feesInPercent2*.01)*mid2Rate)), mid1Precision)
            tempStartCount =  round((startRate*tempMid1Count*(1+feesInPercent1*.01)), startPrecision)
            tempMid2Count = truncate((mid2Rate*tempMid1Count*(1-feesInPercent2*.01)), min(newMid2Precision,mid2Precision))
            tempEndCount = truncate(((tempMid2Count/mid2RateFrom3)*(1-feesInPercent3*.01)), newStartPrecision)
        
    if ( ( 0 < tempStartCount <= bal1 ) and ( 0 < tempMid1Count <= mid1Count ) and ( tempMid2Count > 0 )):
        try:
            if (0 < tempEndCount <= endCount):
                startCount, mid1Count, mid2Count, endCount = tempStartCount, tempMid1Count, truncate(tempMid2Count,min(newMid2Precision,mid2Precision)), tempEndCount
            else:
                print(f"{first}-{second}-{third}: Order3 Error")
                return None
        except Exception:
            if (0 < tempMid2Count <= mid2CountFrom3):
                startCount, mid1Count, mid2Count, endCount = tempStartCount, tempMid1Count, truncate(tempMid2Count,min(newMid2Precision,mid2Precision)), tempEndCount
            else:
                print(f"{first}-{second}-{third}: Order3 Error")
                return None
    else:
        print(f"{first}-{second}-{third}: Order3 Error")
        return None
    
    if ( startCount <= startCountMinLimit ):
        print(f"{first}-{second}-{third}: {first} below Min Volume")
        return None
    elif ( mid2Count <= mid2CountMinLimit ):
        print(f"{first}-{second}-{third}: {third} below Min Volume")
        return None
    
#     print((endCount - startCount),first)
#     if (third+first) in marks:
#         print(((mid1Count*mid2Rate)-(mid2Count)),third)
#         print(((endRate*((mid1Count*mid2Rate)-(mid2Count))*(1-feesInPercent3*.01))+(endCount - startCount)),first)
#     elif (first+third) in marks:
#         print(((mid1Count*mid2Rate)-(endCount/mid2RateFrom3)),third)
#         print((((((mid1Count*mid2Rate)-(endCount/mid2RateFrom3))/mid2RateFrom3)*(1-feesInPercent3*.01))+(endCount - startCount)),first)
    
    
    if (endCount > startCount):
        order1 = [(second+first),'buy',mid1Count,startRate]
        order2 = [(second+third),'sell',mid1Count,mid2Rate]
        if (third+first) in marks:
            order3 = [(third+first),'sell',mid2Count,endRate]
        elif (first+third) in marks:
            order3 = [(first+third),'buy',endCount,mid2RateFrom3]
        print(f"{first}-{second}-{third}: Signal Succeeded")
        return ((endCount-startCount),first),(order1,order2,order3)
    else:
        print(f"{first}-{second}-{third}: Signal Failed")
#         return startCount, mid1Count, mid2Count, endCount
        return None

def getFeesInPercent(ecode):
    if ecode in ['B']:
        return .1
    elif ecode in ['H','HB','I']:
        return .2
    else:
        return .2