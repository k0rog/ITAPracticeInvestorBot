import boto3
import exchange_connector


class DynamoConnector:
    ALLOWED_TABLE_NAMES = ['shares', 'users']
    KEY_SCHEMAS = {
        'shares': [{
            'AttributeName': 'ticker',
            'KeyType': 'HASH'
        }],
        'users': [{
            'AttributeName': 'user_id',
            'KeyType': 'HASH'
        }]
    }
    ATTRIBUTE_DEFINITIONS = {
        'shares': [{
            'AttributeName': 'ticker',
            'AttributeType': 'S'
        }],
        'users': [{
            'AttributeName': 'user_id',
            'AttributeType': 'N'
        }]
    }

    def __init__(self, access_key_id, secret_access_key, region):
        self.db = boto3.resource('dynamodb', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
                                 region_name=region)

    def check_tables(self):
        existing_table_names = [table.name for table in self.db.tables.all()]
        for table_name in self.ALLOWED_TABLE_NAMES:
            if table_name not in existing_table_names:
                self._create_table(table_name)
                self.pool_data(table_name)
            elif self.get_table(table_name).item_count == 0:
                self.pool_data(table_name)

    def pool_data(self, table_name):
        if table_name == 'shares':
            shares = exchange_connector.get_shares()
            table = self.get_table(table_name)
            with table.batch_writer() as batch:
                for share in shares:
                    batch.put_item(Item=share)

    def get_table(self, table_name):
        return self.db.Table(table_name)

    def check_item(self, table_name, hash_value, sort_value=None):
        table = self.get_table(table_name)
        key = self._get_table_key(table_name, hash_value, sort_value)
        response = table.get_item(
            Key=key
        )

        try:
            return response['Item']
        except KeyError:
            return False

    def get_item(self, table_name, hash_value, sort_value=None):
        table = self.get_table(table_name)
        key = self._get_table_key(table_name, hash_value, sort_value)
        return table.get_item(
            Key=key
        )['Item']

    def get_table_items(self, table_name, page=None, page_size=50):
        table = self.get_table(table_name)
        response = table.scan()
        items = response['Items']
        if not isinstance(page, int):
            return items

        item_count = response['Count']
        if item_count % page_size != 0:
            page_count = item_count // page_size + 1
        else:
            page_count = item_count // page_size

        if page > page_count or page < 1:
            raise WrongPageException(page, page_count)

        next_page = page + 1 if page + 1 <= page_count else None
        prev_page = page - 1 if page > 1 else None

        return items[(page-1)*page_size:page*page_size], prev_page, next_page, page_count

    def add_item(self, table_name, item):
        table = self.get_table(table_name)
        table.put_item(Item=item)

    def get_new_item(self, table_name, hash_value, sort_value, fields):
        key = self._get_table_key(table_name, hash_value, sort_value)

        for field, field_type in fields.items():
            key[field] = field_type()

        return key

    def _get_table_key(self, table_name, hash_value, sort_value):
        schema = self.KEY_SCHEMAS[table_name]
        hash_key = schema[0]['AttributeName']
        sort_key = schema[1]['AttributeName'] if sort_value else None

        key = {key: value for key, value in ((hash_key, hash_value), (sort_key, sort_value)) if value is not None}

        return key

    def update_item(self, table_name, hash_value, sort_value=None, **kwargs):
        table = self.get_table(table_name)

        kwargs = {key: value for key, value in kwargs.items() if value is not None}
        kwargs['Key'] = self._get_table_key(table_name, hash_value, sort_value)

        table.update_item(**kwargs)

    def _create_table(self, table_name):
        table = self.db.create_table(
            TableName=table_name,
            KeySchema=self.KEY_SCHEMAS[table_name],
            AttributeDefinitions=self.ATTRIBUTE_DEFINITIONS[table_name],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        table.meta.client.get_waiter('table_exists').wait(TableName=table)

        return table


class WrongPageException(Exception):
    def __init__(self, page, last_page):
        super().__init__(f'Вы ввели страницу {page}. Введите значение между 1 и {last_page}')
