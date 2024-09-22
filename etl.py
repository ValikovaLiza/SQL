import pandas as pd
import psycopg2
from psycopg2 import sql

# Настройки подключения к базе данных PostgreSQL
db_config = {
    'dbname': 'my_etl_database',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
}

def load_data_to_raw_table(data, db_config):
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Создание временной таблицы, если она не существует
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl.raw_table (
        ID SERIAL PRIMARY KEY,
        userID INTEGER,
        Track VARCHAR(255),
        artist VARCHAR(255),
        genre VARCHAR(255),
        City VARCHAR(255),
        time TIMESTAMP,
        Report_date DATE,
        Weekday VARCHAR(50)
    );
    """)
    
    # Загрузка данных из DataFrame в таблицу PostgreSQL
    for index, row in data.iterrows():
        cursor.execute("""
        INSERT INTO etl.raw_table (userID, Track, artist, genre, City, time, Report_date, Weekday)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """, (row['userID'], row['Track'], row['artist'], row['genre'], row['City'], row['time'], row['Report_date'], row['Weekday']))
    
    # Сохранение изменений и закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()

def transform_data(start_date, end_date, db_config):
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Вызов процедуры трансформации
    cursor.execute("CALL etl.transform_data(%s, %s);", (start_date, end_date))
    
    # Сохранение изменений и закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()

def load_data_to_target_table(start_date, end_date, db_config):
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Выполнение загрузки данных
    cursor.execute("""
    INSERT INTO etl.target_table (userID, Track, artist, genre, City, time, Report_date, Weekday)
    SELECT userID, Track, artist, genre, City, time, Report_date, Weekday
    FROM etl.temp_table
    WHERE Report_date BETWEEN %s AND %s;
    """, (start_date, end_date))
    
    # Сохранение изменений и закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()

def etl_process(file_path, start_date, end_date, db_config):
    # Шаг 1: Extract - Извлечение данных из файла и загрузка в raw_table
    data = pd.read_csv(file_path)
    load_data_to_raw_table(data, db_config)

    # Шаг 2: Transform - Трансформация данных из raw_table в temp_table
    transform_data(start_date, end_date, db_config)

    # Шаг 3: Load - Загрузка данных из temp_table в target_table
    load_data_to_target_table(start_date, end_date, db_config)

# Выполнение полного ETL-процесса
etl_process('music.xlsx - Лист1.csv', '2024-01-01', '2024-01-31', db_config)
