
Скачаем docker-compose с настройками airflow
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.4.2/docker-compose.yaml'
```

Запустим Airflow
```bash
docker-compose -p airflow_ up -d
```

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