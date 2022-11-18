from time import sleep
from auth_req import makeAuthReq
import pandas as pd

def getBalDict(endPoint, auth):
    while True:
        balDictReq = makeAuthReq(endPoint,{}, auth)
        if not (balDictReq.status_code==200):
            print("Balance Error")
            print(balDictReq)
            sleep(0.5)
        else:
            break
    return balDictReq.json()

def getBal(balDict, symb):
    balDf = pd.DataFrame(balDict)
    return float(balDf[balDf.currency==symb].balance.values[0])