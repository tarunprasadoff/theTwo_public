import json
import hmac
import hashlib
from time import time
from requests import post

def makeAuthReq(url,body,auth):

    timeStamp = int(round(time() * 1000))

    body["timestamp"] = timeStamp

    json_body = json.dumps(body, separators = (',', ':'))

    signature = hmac.new(auth.secret_bytes, json_body.encode(), hashlib.sha256).hexdigest()

    headers = {
        'Content-Type': 'application/json',
        'X-AUTH-APIKEY': auth.key,
        'X-AUTH-SIGNATURE': signature
    }

    response = post(url, data = json_body, headers = headers)
    return response