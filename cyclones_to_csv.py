import argparse
from typing import List

import pandas as pd

from sqlalchemy import create_engine

class CyclonesToCSV:
    def __init__(self, number_of_month: int):
        self.number_of_month = number_of_month
        self.engine = create_engine('postgresql://postgres:postgres@localhost:1/postgres')

    def get_days_of_month(self) -> List[int]:
        """"""
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
        """"""

        days = self.get_days_of_month()
        counter_of_day = []
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
            counter_of_day.append(len(df))
            name_of_day = df.date.unique()
            for date in name_of_day:
                df_ = df[df.date == date]
                df_.to_csv(f'cyclones_per_day/cyclones_{date}.csv', index=False)

        if sum(counter_of_day) == 0:
            print('Нет циклонов за этот месяц')

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-n",
        help="Номер месяца",
    )

    args = vars(ap.parse_args())

    object_ = CyclonesToCSV(number_of_month=args['n'])

    object_.write_days_to_csv()

    # load_data_to_db(path_to_file=args['p'])
