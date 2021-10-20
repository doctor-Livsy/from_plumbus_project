import asyncio
# from binance.client import Client as BinanceClient
# from utils.db_api.postgresql import Database


class ClientsPool:
    '''
    Класс для инициализации и управления торговыми клиентами. 
    При старте бота ClientsPool выгружает из базы данных зарегестрированных клиентов указанной биржи,
    а также их торговые параметры и создает пул клиентов
    '''

    def __init__(self, exchange, database: Database, loop: asyncio.AbstractEventLoop):
        self.exchange = exchange
        self.__clients: dict = loop.run_until_complete(
            self.load_all_clients(database=database,
                                  exchange=self.exchange))
        loop.run_until_complete(self.load_trade_parameters(self,
                                                           database=database,
                                                           exchange=self.exchange))

    @staticmethod
    async def load_all_clients(database: Database, exchange: str):

        # Загрузка записей из таблицы some_table в формате словаря
        api_records = await database.select_from_table('user_id', 'api_key',
                                                       's_key',
                                                       table='some_table',
                                                       exchange=exchange)

        # Создание итогового словаря с объектами клиентов
        clients = {}
        for i in range(len(api_records['user_id'])):
            user_id: str = str(api_records['user_id'][i])
            if exchange == 'Binance'.upper():
                clients[user_id] = {'CLIENT': BinanceClient(api_records['api_key'][i],
                                                            api_records['s_key'][i]),
                                    'SETTINGS': {}}
            if exchange == 'bybit'.upper():
                clients[user_id] = 'bybit'  # TODO: Вставить сюда клиент байбита

        return clients

    @staticmethod
    async def load_trade_parameters(self, database: Database, exchange: str):
        # Извлечение данных о параметрах из базы данных
        trade_parameters = await database.select_from_table('user_id',
                                                            'trade_pair',
                                                            'leverage',
                                                            'margin_value',
                                                            table='some_table',
                                                            exchange=exchange)

        # Рефакторинг данных для дальнейшего добавления в словарь __clients
        user_list, settings = self.create_settings_dict(trade_parameters)
        # Загрузка параметров в словарь __clients
        for user_id in user_list:
            user_settings = {'SETTINGS': settings[str(user_id)]}
            self.__clients[str(user_id)].update(user_settings)

    @staticmethod
    def create_settings_dict(trade_parameters: dict):
        # Создания словаря с торговыми параметрами
        settings = {str(user_id): {} for user_id in trade_parameters['user_id']}

        # Заполнение параметров
        for i in range(len(trade_parameters['user_id'])):
            user_id: str = str(trade_parameters['user_id'][i])

            # Формирование словаря с параметрами для тикера
            values = {'leverage': trade_parameters['leverage'][i],
                      'margin_value': trade_parameters['margin_value'][i]}
            ticker_parameters = {trade_parameters['trade_pair'][i]: values}

            # Заполнение данных в итоговый словарь
            settings[user_id].update(ticker_parameters)

        user_list: list = trade_parameters['user_id']
        return user_list, settings

    def client_start(self, user_id):
        self.__clients.update({str(user_id): {'CLIENT': None,
                                              'SETTINGS': {}}})

    def add_client(self, user_id: str, api_key: str, s_key: str):
        self.__clients[str(user_id)] = {'CLIENT': BinanceClient(api_key, s_key),
                                        'SETTINGS': {}}

    def get_client(self, user_id):
        client = self.__clients[str(user_id)]['CLIENT']
        return client

    def delete_client(self, user_id):
        self.__clients[str(user_id)]['CLIENT'] = None

    def update_user_settings(self, user_id, ticker, leverage, margin_value):
        new_ticker_parameters = {str(ticker): {'leverage': leverage,
                                               'margin_value': margin_value}}
        self.__clients[str(user_id)]['SETTINGS'].update(new_ticker_parameters)

    def get_user_settings(self, user_id):
        settings = self.__clients[str(user_id)]['SETTINGS']
        return settings

    def delete_ticker_settings(self, user_id, ticker):
        self.__clients[str(user_id)]['SETTINGS'].pop(ticker)

    def get_all_clients(self):
        return self.__clients

    def clients_is_empty(self):
        empty = True if self.__clients == {} else False
        return empty
