import os


TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
AWS_ACCESS_KEY_ID = os.environ.get('MY_AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('MY_AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.environ.get('MY_AWS_DEFAULT_REGION')

EXCHANGE_ENDPOINT_URL = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json'
