import pandas as pd
import psycopg2
from psycopg2 import sql

# Настройки подключения к базе данных
db_config = {
    'dbname': 'my_etl_database',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}
