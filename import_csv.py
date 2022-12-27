import pandas as pd
import psycopg2
from config import host, user, password, db_name

path_file = r'/home/an/dl/MSP/msp.csv'
wFile = pd.read_csv(path_file,nrows=100)
# print(wFile.head(10))     # выводит заголовок таблицы
print(wFile.to_csv(None))   # выводит полную таблицу

print(wFile.info())         # информация по полям

conn = psycopg2.connect(database=db_name, host=host, user=user, password=password)
conn.autocommit = True
cursor = conn.cursor()