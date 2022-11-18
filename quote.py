import asyncio
import aiohttp

async def groupQuotes(marks, pairs, ecodes, endPoint):
    fq={}
    client = aiohttp.ClientSession()
    actexcsPairs = []
    for pair in pairs:
        if (pair[2]+pair[0]) in marks:
            if not ([pair[2],pair[0]] in actexcsPairs):
                actexcsPairs.append([pair[2],pair[0]])
        elif (pair[0]+pair[2]) in marks:
            if not ([pair[0],pair[2]] in actexcsPairs):
                actexcsPairs.append([pair[0],pair[2]])
    fetch = await asyncio.gather(*[*[getDepthAsync(p[0],p[1], client, ecodes, endPoint) for p in actexcsPairs],*[fetchDepthQuotes(pair[0], pair[1], pair[2], client, ecodes, endPoint) for pair in pairs]])
    for i in range(len(actexcsPairs)):
        if actexcsPairs[i][0] not in fq.keys():
            fq[actexcsPairs[i][0]] = {}
        fq[actexcsPairs[i][0]][actexcsPairs[i][1]] = fetch[i]        
    for i in range(len(actexcsPairs),len(fetch)):
        if pairs[i-len(actexcsPairs)][1] not in fq.keys():
            fq[pairs[i-len(actexcsPairs)][1]] = {}
        for k in fetch[i].keys():
            fq[pairs[i-len(actexcsPairs)][1]][k] = fetch[i][k]
    await client.close()
    return fq

async def getDepthAsync(crypto, base, client, ecodes, endPoint):
    try:
        pairSymb = ecodes[(crypto+base)] + '-' + crypto + '_' + base
        req = await client.get(endPoint + pairSymb)
        reqJson = await req.json()
        if not (req.status==200):
            print(pairSymb,"Request Status Error", req, reqJson)
            reqJson = {'bids':None,'asks':None}
    except Exception as l:
        print(pairSymb,"Request Json Error", l)
        return {'buy':None,'sell':None}
        
    if reqJson == None:
        return {'buy':None,'sell':None}
    
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

async def fetchDepthQuotes(first, second, third, client, ecodes, endPoint):
    q1, q2 = await asyncio.gather(getDepthAsync(second,first,client, ecodes, endPoint),getDepthAsync(second,third,client, ecodes, endPoint))
    return {first: q1, third: q2}