import asyncio
import os
import random
import time

import asyncpg
import mimesis
from asyncpg import Connection

DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
DB_HOST = os.getenv('POSTGRES_HOST')
DB_PORT = os.getenv('POSTGRES_PORT')


def query_logger(record):
    print(record.query)


async def db_connection() -> Connection:
    connection: Connection = await asyncpg.connect(user=DB_USER, password=DB_PASS,
                                                   database=DB_NAME, host=DB_HOST, port=DB_PORT)
    connection.add_query_logger(query_logger)
    return connection


async def update_table():
    connection = await db_connection()

    sql3 = '''  SELECT full_names.name as full_name, short_names.status
                FROM public.full_names 
                inner join short_names on
                regexp_replace(full_names.name, '\.\w+\.?\w+?', '') = short_names.name
                '''

    request = await connection.fetch(sql3)
    values = [(item.get('full_name'), item.get('status')) for item in request]
    sql_update = f'''UPDATE full_names SET status = $2 WHERE name = $1'''
    async with connection.transaction():
        try:
            await connection.executemany(sql_update, values)
        except Exception as _ex:
            print(_ex)


async def main():
    start = time.time()
    await fill_tables()
    print(f'Заполнение базы данных - {time.time() - start}')
    start2 = time.time()
    # await update_table()
    print(f'Обновление базы данных - {time.time() - start2}')
    print(f'Время работы скрипта - {time.time() - start}')


async def fill_tables():
    short_records = 700000
    full_records = 500000

    connection = await db_connection()

    create_short_sql = '''CREATE TABLE IF NOT EXISTS public.short_names
                        (
                            name character varying(30) COLLATE pg_catalog."default" NOT NULL,
                            status bigint,
                            CONSTRAINT short_names_pkey PRIMARY KEY (name)
                        )'''

    create_full_sql = '''CREATE TABLE IF NOT EXISTS public.full_names
                        (
                            name character varying(30) COLLATE pg_catalog."default" NOT NULL,
                            status bigint,
                            CONSTRAINT full_names_pkey PRIMARY KEY (name)
                        )'''
    await connection.execute(create_short_sql)
    await connection.execute(create_full_sql)

    sql_short = f'''INSERT INTO short_names (name, status) VALUES ($1, $2)'''
    sql_full = f'''INSERT INTO full_names (name, status) VALUES ($1, $2)'''

    count_short_sql = '''SELECT COUNT(*) FROM short_names'''
    count_full_sql = '''SELECT COUNT(*) FROM full_names'''
    count_full = await connection.fetchval(count_full_sql)
    count_short = await connection.fetchval(count_short_sql)

    if count_full == full_records and count_short == short_records:
        return

    short_list = []
    full_list = []
    file = mimesis.File(seed=3)
    names = [file.file_name(file_type=None) for _ in range(10)]

    for i in range(short_records):
        file_name = random.choice(names)
        randint = str(i)
        short_file_name = file_name.split('.')[0] + randint
        file_name = short_file_name + '.' + '.'.join(file_name.split('.')[1:])
        short_list.append((short_file_name, random.choice(range(6))))

        if i < full_records:
            full_list.append((file_name, None))

    async with connection.transaction():
        try:
            await connection.executemany(sql_short, short_list)
        except Exception as _ex:
            print(_ex)
    async with connection.transaction():
        try:
            await connection.executemany(sql_full, full_list)
        except Exception as _ex:
            print(_ex)


if __name__ == '__main__':
    asyncio.run(main())
