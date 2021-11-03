import telebot
from telebot import types
from dynamo_connector import DynamoConnector, WrongPageException
import bot_config
import bot_messages
from utils import isint
from models import User, Exchange
from boto3.dynamodb.conditions import Attr


bot = telebot.TeleBot(bot_config.TELEGRAM_TOKEN, threaded=False)


def notify_users():
    for user in User.objects.all():
        shares = get_user_shares(user['user_id'], change=True)

        if not len(shares):
            continue

        total_change = sum([share['change'] for share in shares])

        if total_change < 0:
            response = f'Изменение стоимости активов: {total_change}'
        else:
            response = f'Изменение стоимости активов: +{total_change}'

        bot.send_message(user['user_id'], response)


def lambda_handler(message, context):
    try:
        if 'source' in message:
            Exchange.update_shares()
            notify_users()
        else:
            json_string = message['body']

            update = telebot.types.Update.de_json(json_string)

            bot.process_new_updates([update])
    except Exception as e:
        print(e)

    return {"statusCode": 200}


@bot.message_handler(func=lambda message: True, commands=['start'])
def start_handler(message):
    db = DynamoConnector(bot_config.AWS_ACCESS_KEY_ID, bot_config.AWS_SECRET_ACCESS_KEY, bot_config.AWS_DEFAULT_REGION)
    db.check_tables()

    bot.set_my_commands([
        types.BotCommand('help', bot_messages.help_description),
        types.BotCommand('add', bot_messages.add_description),
        types.BotCommand('ticker_list', bot_messages.ticker_list_description),
        types.BotCommand('detail', bot_messages.detail_description),
        types.BotCommand('delete', bot_messages.delete_description),
        types.BotCommand('update', bot_messages.update_description),
        types.BotCommand('my_tickers', bot_messages.my_tickers_description),
        types.BotCommand('my_investment_portfolio', bot_messages.my_investment_portfolio_description),
        types.BotCommand('cancel', bot_messages.cancel_description),
    ])

    user = User.objects.get_or_create(message.from_user.id)

    bot.send_message(user.id, bot_messages.start_message)


def dialog_function_wrapper(error_message, allowed_commands=None):
    if allowed_commands is None:
        allowed_commands = []

    def outer(func):
        def wrapper(message, *args, **kwargs):
            if message.text.startswith('/cancel'):
                return
            elif message.text in allowed_commands:
                bot.process_new_messages([message])
                return

            result = func(message, *args, **kwargs)

            if not result:
                msg = bot.send_message(message.from_user.id, error_message)
                if args:
                    bot.register_next_step_handler(msg, wrapper, *args)
                else:
                    bot.register_next_step_handler(msg, wrapper)
                return

            return result

        return wrapper

    return outer


def get_user_shares(user_id, change=False):
    user = User.objects.get(user_id)

    user_shares = user.get_shares()
    user_tickers = user.get_tickers()

    exchange_shares = Exchange.objects.filter(FilterExpression=Attr('ticker').is_in(user_tickers),
                                              ProjectionExpression='ticker,lot_price,lot_price_change')

    shares = []
    for share in exchange_shares:
        share['users_capitalization'] = share['lot_price'] * user_shares[share['ticker']]['amount']
        share['amount'] = user_shares[share['ticker']]['amount']
        if change:
            share['change'] = round(share['amount'] * share['lot_price_change'])
        shares.append(share)

    return shares


@bot.message_handler(func=lambda message: True, commands=['my_investment_portfolio'])
def my_investment_portfolio_command(message):
    shares = get_user_shares(message.from_user.id)

    total_price = sum([share['users_capitalization'] for share in shares])

    bot.send_message(message.from_user.id, f'Стоимость твоего портфеля на данный момент = {total_price}₽')


@bot.message_handler(func=lambda message: True, commands=['my_tickers'])
def my_tickers_command(message):
    shares = get_user_shares(message.from_user.id)

    response = '\n'.join([f'{share["ticker"]} - {share["amount"]} - {share["users_capitalization"]}₽'
                          for share in shares])

    bot.send_message(message.from_user.id, f'Список твоих тикеров:\n{response}\n'
                                           f'Добавить новый можно командой /add\n'
                                           f'Удалить можно командой /delete')


# detail dialog
@bot.message_handler(func=lambda message: True, commands=['detail'])
def detail_command(message):
    bot.send_message(message.from_user.id, bot_messages.detail_command)
    bot.register_next_step_handler(message, detail_get_ticker)


@dialog_function_wrapper(bot_messages.detail_get_ticker_error, ['/ticker_list'])
def detail_get_ticker(message):
    if message.text.isalpha():
        tickers = Exchange.get_tickers()

        if message.text.upper() in tickers:
            share = Exchange.objects.get(message.text.upper())
            share = share.get_data()

            response = f'Вы запросили тикер {share["ticker"]}\n' \
                       f'Это тикер компании {share["name"]}\n' \
                       f'Цена за одну акцию: {share["price"]}\n' \
                       f'Размер лота: {share["lot_size"]}\n' \
                       f'Цена лота: {share["lot_price"]}'

            user = User.objects.get(message.from_user.id)
            user_shares = user.get_shares()

            if share['ticker'] in user_shares.keys():
                amount = user.get_shares()[share['ticker']]['amount']

                share['amount'] = amount
                response += f'\n\nКоличество ваших лотов: {share["amount"]}\n' \
                            f'Общая цена ваших лотов: {share["amount"] * share["lot_price"]}'

            bot.send_message(message.from_user.id, response)
        else:
            bot.send_message(message.from_user.id,
                             bot_messages.detail_no_ticker_error)
            bot.register_next_step_handler(message, detail_get_ticker)
        return True
# detail dialog end


# addition dialog
@bot.message_handler(func=lambda message: True, commands=['add'])
def add_command(message):
    bot.send_message(message.from_user.id, bot_messages.add_command)
    bot.register_next_step_handler(message, add_get_ticker)


@dialog_function_wrapper(bot_messages.add_get_ticker_error, ['/ticker_list'])
def add_get_ticker(message):
    if message.text.isalpha():
        tickers = Exchange.get_tickers()

        user = User.objects.get(message.from_user.id)
        user_tickers = user.get_tickers()

        if message.text.upper() in user_tickers:
            bot.send_message(message.from_user.id,
                             bot_messages.add_already_ticker_error)
            bot.register_next_step_handler(message, add_get_ticker)
        elif message.text.upper() in tickers:
            bot.send_message(message.from_user.id, bot_messages.add_request_lot_amount)
            bot.register_next_step_handler(message, add_get_lot_amount, message.text)
        else:
            bot.send_message(message.from_user.id,
                             bot_messages.add_no_ticker_error)
            bot.register_next_step_handler(message, add_get_ticker)
        return True


@dialog_function_wrapper(bot_messages.add_get_lot_amount_error)
def add_get_lot_amount(message, ticker):
    if message.text.isdigit() and isint(message.text) and int(message.text) > 0:
        user = User.objects.get(message.from_user.id)

        user.add_ticker(ticker, int(message.text))

        bot.send_message(message.from_user.id,
                         f'Тикер {ticker} в количестве {message.text} добавлен\n'
                         f' Удалить тикер из своего списка можно командой /delete\n'
                         f'Изменить количество лотов можно командой /update')
        return True
# addition dialog end


# delete dialog
@bot.message_handler(func=lambda message: True, commands=['delete'])
def delete_command(message):
    bot.send_message(message.from_user.id, bot_messages.delete_command)
    bot.register_next_step_handler(message, delete_get_ticker)


@dialog_function_wrapper(bot_messages.delete_get_ticker_error, ['/my_tickers'])
def delete_get_ticker(message):
    if message.text.isalpha():
        user = User.objects.get(message.from_user.id)

        user_tickers = user.get_tickers()

        if message.text.upper() in user_tickers:
            user.delete_ticker(message.text)
            bot.send_message(message.from_user.id, f'Тикер {message.text} удалён')
        else:
            bot.send_message(message.from_user.id, bot_messages.delete_no_ticker_error)
            bot.register_next_step_handler(message, delete_get_ticker)
        return True
# delete dialog end


# update dialog
@bot.message_handler(func=lambda message: True, commands=['update'])
def update_command(message):
    bot.send_message(message.from_user.id, bot_messages.update_command)
    bot.register_next_step_handler(message, update_get_ticker)


@dialog_function_wrapper(bot_messages.update_get_ticker_error, ['/my_tickers'])
def update_get_ticker(message):
    if message.text.isalpha():
        user = User.objects.get(message.from_user.id)

        user_tickers = user.get_tickers()

        if message.text.upper() in user_tickers:
            bot.send_message(message.from_user.id, bot_messages.update_request_lot_amount)
            bot.register_next_step_handler(message, update_get_lot_amount, message.text)
        else:
            bot.send_message(message.from_user.id, bot_messages.update_no_ticker_error)
            bot.register_next_step_handler(message, update_get_ticker)
        return True


@dialog_function_wrapper(bot_messages.update_get_lot_amount_error, ['/my_tickers'])
def update_get_lot_amount(message, ticker):
    if message.text.isdigit() and isint(message.text):
        user = User.objects.get(message.from_user.id)

        user.update_ticker(ticker, int(message.text))

        bot.send_message(message.from_user.id,
                         f'Тикер {ticker} обновлён\n'
                         f'Удалить тикер из своего списка можно командой /delete')
        return True
# update dialog end


@bot.message_handler(func=lambda message: True, commands=['cancel'])
def cancel_command(message):
    bot.send_message(message.from_user.id, bot_messages.cancel_command)


def send_ticker_list(user_id, message_text, page=1):
    if len(message_text.split()) > 1 and message_text.split()[1].isdigit() and isint(message_text.split()[1]):
        page = int(message_text.split()[1])

    try:
        tickers, prev_page, next_page, page_count = Exchange.objects.paginate(page)
        tickers = [share['ticker'] for share in tickers]
    except WrongPageException as e:
        bot.send_message(user_id, str(e))
        return

    tickers = '\n'.join(tickers)

    markup = None
    if prev_page or next_page:
        markup = types.InlineKeyboardMarkup()
        if prev_page:
            markup.add(types.InlineKeyboardButton('Предыдущая страница',
                                                  callback_data=f'ticker_list_prev_page {prev_page}'))
        if next_page:
            markup.add(types.InlineKeyboardButton('Следующая страница',
                                                  callback_data=f'ticker_list_next_page {next_page}'))

    bot.send_message(user_id, f'Список тикеров:\n{tickers}\n'
                              f'Страница {page}/{page_count}\n'
                              f'Вы можете узнать больше информации про конкретный тикер командой /detail',
                     reply_markup=markup)


@bot.message_handler(func=lambda message: True, commands=['ticker_list'])
def ticker_list_command(message):
    send_ticker_list(message.from_user.id, message.text)


@bot.callback_query_handler(func=lambda call: True)
def callback_worker(call):
    if call.data.startswith('ticker_list_prev_page') or call.data.startswith('ticker_list_next_page'):
        send_ticker_list(call.from_user.id, f'/ticker_list {call.data.split()[1]}')
        bot.answer_callback_query(call.id)


@bot.message_handler(func=lambda message: True, commands=['help'])
def help_command(message):
    bot.send_message(message.from_user.id, bot_messages.help_command)


@bot.message_handler(content_types=['text'])
def message_handler(message):
    if message.text.startswith('/'):
        bot.send_message(message.from_user.id, 'Нет такой команды')
