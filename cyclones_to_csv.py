import argparse
from typing import List

import pandas as pd

from sqlalchemy import create_engine

class CyclonesToCSV:
    def __init__(self, number_of_month: int):
        self.number_of_month = number_of_month
        self.engine = create_engine('postgresql://postgres:postgres@localhost:1/postgres')

    def get_days_of_month(self) -> List[int]:
        """
        Получаем список дней за выбранный месяц.

        :return: Возвращает список дней, в которых были циклоны
        """
        query = f'''
        SELECT 
            DISTINCT days::integer 
        FROM 
        (
            SELECT 
                    id, 
                    date,
                    date_part('day', to_date(date::text, 'YYYYMMDD')) AS days,
                    max(time) AS time
                FROM 
                    cyclones
                WHERE 
                    date_part('month', to_date(date::text, 'YYYYMMDD')) = {self.number_of_month}
                GROUP BY 
                    id, 
                    date,
                    date_part('day', to_date(date::text, 'YYYYMMDD'))
        ) t
        '''

        df = pd.read_sql(sql=query, con=self.engine)

        return df.days.to_list()

    def write_days_to_csv(self):
        """
        Записывает каждый день в отдельный .csv файл
        """

        # Получаем список дней за указанный месяц
        days = self.get_days_of_month()
        # Создаем счетчик дней со значениями, для дальнейшего сообщения пользователю
        counter_of_day = []
        # Запускаем цикл по каждому дню в месяце, по которым есть значения
        for day in days:
            query = f'''
            WITH pre_main AS 
            (
                SELECT 
                    id, 
                    date, 
                    max(time) AS time
                FROM 
                    cyclones
                WHERE
                    to_date(date::text, 'YYYYMMDD') >= '2013-01-01'::date
                    AND date_part('month', to_date(date::text, 'YYYYMMDD')) = {self.number_of_month}
                    AND date_part('day', to_date(date::text, 'YYYYMMDD')) = {day}
                GROUP BY 
                    id, 
                    date
            )
            , main AS 
            (
            SELECT 
                m.id, 
                m.date, 
                cyc.status  
            FROM 
                pre_main AS m
                LEFT JOIN cyclones AS cyc 
                    ON m.id = cyc.id 
                    AND m.date = cyc.date
                    AND m.time = cyc.time
            )
            SELECT 
                *
            FROM 
                main
            '''

            df = pd.read_sql(sql=query, con=self.engine)
            # добавляем значение в наш счетчик
            counter_of_day.append(len(df))
            # Смотрим уникальные даты за данный месяц и день
            name_of_day = df.date.unique()
            # Пробегаемся по каждой дате и помещаем её в отдельный .csv файл
            for date in name_of_day:
                df_ = df[df.date == date]
                df_.to_csv(f'cyclones_per_day/cyclones_{date}.csv', index=False)
        # Уведомление пользователю, если за данный месяц нет дней с циклонами
        if sum(counter_of_day) == 0:
            print('Нет циклонов за этот месяц')
        else:
            print('Данные находятся в папке cyclones_per_day')

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-n",
        help="Номер месяца",
    )

    args = vars(ap.parse_args())
    number_of_month = int(args['n'])

    if isinstance(number_of_month, int) and 12 >= number_of_month >= 1:
        object_ = CyclonesToCSV(number_of_month=number_of_month)
        object_.write_days_to_csv()
    else:
        print('Необходимо передать номер месяца (1, 2, ..., 12)')




