import asyncio
import asyncpg
from data import config


class Database:
    ''' API к базе данных Postgresql с асинхронными запросами. '''

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.pool = loop.run_until_complete(
            asyncpg.create_pool(database=config.DATABASE,
                                user=config.PGUSER,
                                password=config.PGPASSWORD
                                ))  # host=config.PGHOST
        loop.run_until_complete(self.create_users_table())
        loop.run_until_complete(self.create_api_keys_table())
        loop.run_until_complete(self.create_trade_parameters_table())

    @staticmethod
    def format_args(sql, parameters: dict):
        sql += " AND ".join([f"{item} = ${num}" for num, item in enumerate(parameters, start=1)])
        return sql, tuple(parameters.values())

    @staticmethod
    def args_to_string(*args):
        string = ''
        for arg in args:
            string += arg + ', '
        return string[:-2]

    @staticmethod
    def parameters_to_string(dictionary: dict):
        string = ''
        for key, value in zip(dictionary.keys(), dictionary.values()):
            string += f'{key} = {value}, '

        return string[:-2]

    async def create_users_table(self):
        request = """
                CREATE TABLE IF NOT EXISTS some_table(
                column1 INT NOT NULL PRIMARY KEY,
                column2 VARCHAR(255),
                column3 VARCHAR(255),
                UNIQUE(column1, column2))
                """

        async with self.pool.acquire() as connection:
            await connection.execute(request)

    async def create_api_keys_table(self):
        request = """
                CREATE TABLE IF NOT EXISTS some_table(
                column1 INT NOT NULL PRIMARY KEY,
                column2 VARCHAR(255),
                column3 VARCHAR(255),
                column4 VARCHAR(255),
                UNIQUE(column1, column2))
                """

        async with self.pool.acquire() as connection:
            await connection.execute(request)

    async def create_trade_parameters_table(self):
        request = """
                CREATE TABLE IF NOT EXISTS some_table(
                column1 INT NOT NULL,
                column2 VARCHAR(255),
                column3 VARCHAR(255),
                column4 VARCHAR(255),
                column5 INT,
                column6 INT)
                """

        async with self.pool.acquire() as connection:
            await connection.execute(request)

    async def pool_terminate(self):
        await self.pool.termiate()

    async def give_user(self, **kwargs):
        request = "SELECT * FROM some_table WHERE "
        request, parameters = self.format_args(request, kwargs)
        return await self.pool.fetchrow(request, *parameters)

    async def add_user(self, user: int, user_name: str):
        request = "INSERT INTO some_table(column1, column2) VALUES ($1, $2)"
        async with self.pool.acquire() as connection:
            user = await self.give_user(user=user, user_name=user_name)
            if user is None:
                await connection.execute(request, user, user_name)

    async def append_keys(self, user: int, ex: str, ak: str, s: str):
        request = "INSERT INTO some_table(column1, column2, column3, column4) VALUES ($1, $2, $3, $4)"
        async with self.pool.acquire() as connection:
            await connection.execute(request, user, ex, ak, s)

    async def append_trade_pair(self, user: int, ex: str, tp: str,
                                lev: int, mv: int, lm='Isolated'):
        request = """
                     INSERT INTO 
                     some_table(column1, column2, column3, column4, column5, column6)
                     VALUES ($1, $2, $3, $4, $5, $6)
                  """
        async with self.pool.acquire() as connection:
            await connection.execute(request, user, ex, tp, lev, mv, lm,)

    async def update_data(self, new_parameters: dict, table='some_table', **kwargs):
        string_param = self.parameters_to_string(new_parameters)
        request = f'UPDATE {table} SET {string_param} WHERE '
        request, parameters = self.format_args(request, kwargs)
        async with self.pool.acquire() as connection:
            await connection.execute(request, *parameters)

    async def select_api_info(self, **kwargs):
        request = f"SELECT * FROM some_table WHERE "
        request, parameters = self.format_args(request, kwargs)
        return await self.pool.fetchrow(request, *parameters)

    async def select_trade_parameters(self, **kwargs):
        request = "SELECT * FROM some_table WHERE "
        request, parameters = self.format_args(request, kwargs)
        return await self.pool.fetch(request, *parameters)

    async def count_trade_pair(self, **kwargs):
        request = "SELECT Count(some_column) FROM some_table WHERE "
        request, parameters = self.format_args(request, kwargs)
        return await self.pool.fetchval(request, *parameters)

    async def select_from_table(self, *args, table='Users', **kwargs):
        columns = self.args_to_string(*args)
        request = f"SELECT {columns} FROM {table} WHERE "
        request, parameters = self.format_args(request, kwargs)
        list_records = await self.pool.fetch(request, *parameters)

        dict_records = {}
        for arg in args:
            dict_records[arg] = [record[arg] for record in list_records]

        return dict_records

    async def delete_record(self, table='some_table', **kwargs):
        request = f"DELETE FROM {table} WHERE "
        request, parameters = self.format_args(request, kwargs)
        async with self.pool.acquire() as connection:
            await connection.execute(request, *parameters)
