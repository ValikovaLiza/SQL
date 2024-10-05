import pandas as pd
from sqlalchemy import create_engine

csv_file_path = 'C:/Users/liza5/SQL/music.csv'
df = pd.read_csv(csv_file_path)


df['time'] = df['time'].str.replace(',', '.').astype(float)

engine = create_engine('postgresql+psycopg2://postgres:12345@localhost:5432/postgres')


df.to_sql('raw_data', engine, if_exists='replace', index=False)

print("Data loaded successfully into PostgreSQL.")

