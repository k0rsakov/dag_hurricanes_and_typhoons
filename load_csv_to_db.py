import argparse
import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine

def load_data_to_db(path_to_file: str):
    """
    Загружает указанный .csv файл в таблицу `cyclones`

    :param path_to_file: путь к файлу
    """

    df = pd.read_csv(path_to_file)

    engine_str = f'postgresql://postgres:postgres@localhost:1/postgres'
    engine = create_engine(engine_str)

    new_name_columns = df.columns
    new_name_columns = new_name_columns.map(lambda x: x.lower())
    new_name_columns = new_name_columns.map(lambda x: x.replace(' ', '_'))

    df.columns = new_name_columns

    df.to_sql(
        con=engine,
        chunksize=10024,
        index=False,
        if_exists='append',
        name='cyclones',
        schema='public',
        method='multi',
    )

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-p",
        help="Путь к файлу",
    )

    args = vars(ap.parse_args())

    try:
        load_data_to_db(path_to_file=args['p'])
    except ValueError:
        print('Укажите путь к файлу')
    except SyntaxError:
        print('Путь к файлу, должен быть в str формате')
    except FileNotFoundError:
        print('Неверный путь к файлу или наименование')
    except OperationalError:
        print('Проверьте подключение к БД. Нет подключения к БД на 1-ом порту')
    except Exception as ex:
        raise ex
