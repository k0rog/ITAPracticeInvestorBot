from dynamo_connector import DynamoConnector
import bot_config
from boto3.dynamodb.conditions import Attr


class ObjectDoesNotExist(Exception):
    def __init__(self):
        super().__init__('Object does not exist')


class BaseManager:
    db = DynamoConnector(bot_config.AWS_ACCESS_KEY_ID,
                         bot_config.AWS_SECRET_ACCESS_KEY, bot_config.AWS_DEFAULT_REGION)

    def __init__(self, table_name, fields, cls):
        self.table_name = table_name
        self.fields = fields
        self.cls = cls

    def filter(self, **kwargs):
        table = self.db.get_table(self.table_name)

        return table.scan(**kwargs)['Items']

    def all(self):
        return self.db.get_table_items(self.table_name)

    def paginate(self, page):
        return self.db.get_table_items(self.table_name, page)

    def create(self, item):
        self.db.add_item(self.table_name, item)

    def update_item(self, pk, update_expr, attr_values=None, attr_names=None, sort_key=None):
        self.db.update_item(self.table_name, pk,
                            UpdateExpression=update_expr,
                            ExpressionAttributeValues=attr_values,
                            ExpressionAttributeNames=attr_names,
                            sort_key=sort_key)

    def get(self, pk, sort_key=None):
        item = self.db.check_item(self.table_name, pk, sort_key)

        if not item:
            raise ObjectDoesNotExist()

        if sort_key:
            return eval(self.cls)(pk, sort_key)
        return eval(self.cls)(pk)

    def get_or_create(self, pk, sort_key=None):
        try:
            return self.get(pk, sort_key)
        except ObjectDoesNotExist:
            item = self.db.get_new_item(self.table_name, pk, sort_key, self.fields)
            self.create(item)
            return self.get(pk, sort_key)


class Meta(type):
    def __new__(mcs, name, bases, attrs):
        attrs['objects'] = BaseManager(attrs['table_name'], attrs['fields'], name)
        return super().__new__(mcs, name, bases, attrs)


class Model(metaclass=Meta):
    table_name = None
    fields = {}
    objects = BaseManager(table_name, fields, None)
    pk = None
    sort_key = None

    def get_data(self):
        return self.objects.db.get_item(self.table_name, self.pk, self.sort_key)


class User(Model):
    table_name = 'users'
    fields = {
        'tickers': dict
    }

    def __init__(self, user_id):
        self.id = user_id
        self.pk = user_id

    def get_shares(self):
        return self.objects.filter(FilterExpression=Attr('user_id').eq(self.id),
                                   ProjectionExpression='tickers')[0]['tickers']

    def get_tickers(self):
        return list(self.get_shares().keys())

    def add_ticker(self, ticker, amount):
        self.update_ticker(ticker, amount)

    def delete_ticker(self, ticker):
        self.objects.update_item(self.id,
                                 update_expr='REMOVE tickers.#share',
                                 attr_names={'#share': f'{ticker.upper()}'})

    def update_ticker(self, ticker, amount):
        self.objects.update_item(self.id,
                                 update_expr='SET tickers.#share = :share',
                                 attr_names={'#share': f'{ticker.upper()}'},
                                 attr_values={
                                     ':share': {'amount': amount}
                                 })


class Exchange(Model):
    table_name = 'shares'
    fields = {
        'lot_price': int,
        'lot_size': int,
        'name': str,
        'price': float
    }

    def __init__(self, ticker):
        self.ticker = ticker
        self.pk = self.ticker

    @classmethod
    def get_tickers(cls):
        return [share['ticker'] for share in cls.objects.filter(AttributesToGet=['ticker'])]

    @classmethod
    def update_shares(cls):
        cls.objects.db.pool_data(cls.table_name)
