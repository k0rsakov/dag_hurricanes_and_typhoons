# ETL-скрипт

Запустим отдельный контейнер с PostgreSQL на 1-ом порту
```bash
docker run -d --name postgres_ -e POSTGRES_PASSWORD=postgres -p 1:5432 postgres 
```

Создадим в проекте виртуальное окружение для python 
```bash
python -m venv venv
```

Зайдем в созданное виртуальное окружение 
```bash
source venv/bin/activate
```

Обновим `pip`
```bash
python -m pip install pip --upgrade
```

Установим необходимые расширения
```bash
python -m pip install -r requirements.txt
```

Запустим python-скрипты для загрузки данных в БД
```bash
python load_csv_to_db.py -p 'archive/atlantic.csv'
```

```bash
python load_csv_to_db.py -p 'archive/pacific.csv'
```

Для разбиения данных по дням в .csv файлы нужно выполнить команду, где
`number_of_month` – это номер месяца, за который необходима загрузка данных.
```bash
python cyclones_to_csv.py -n number_of_month 
```
