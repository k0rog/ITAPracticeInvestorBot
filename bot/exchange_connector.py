import requests
import bot_config
from decimal import Decimal
import json


ENDPOINT = bot_config.EXCHANGE_ENDPOINT_URL
PARAMS = {
    'iss.meta': ['off'],
    'securities.columns': ['SECID', 'LOTSIZE', 'SECNAME'],
    'marketdata.columns': ['SECID', 'LAST', 'LASTTOPREVPRICE'],
}
URL_PARAMS = '&'.join([f'{key}={",".join(values)}' for key, values in PARAMS.items()])
URL = f'{ENDPOINT}?{URL_PARAMS}'


def get_shares():
    response = requests.get(URL).json()

    securities = response['securities']['data']
    marketdata = response['marketdata']['data']

    shares = []
    for row in zip(securities, marketdata):
        if None in set(row[0]) | set(row[1]):
            continue
        share = {
            'ticker': row[0][0],
            'name': row[0][2],
            'price': row[1][1],
            'lot_size': row[0][1],
            'lot_price': round(row[1][1] * row[0][1]),
            'lot_price_change': row[1][2] * row[0][1]
        }
        share = json.loads(json.dumps(share), parse_float=Decimal)
        shares.append(share)

    return shares
