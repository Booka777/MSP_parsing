import pandas as pd
import psycopg2
from config import host, user, password, db_name
from datetime import datetime, timedelta

# ---------------это пробный блок -----------------
path_file = r'/home/an/dl/MSP/msp.csv'
wFile = pd.read_csv(path_file,nrows=100)
# print(wFile.head(10))     # выводит заголовок таблицы
print(wFile.to_csv(None))   # выводит полную таблицу
print(wFile.info())         # информация по полям
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

wDate = datetime.strptime(input("Введите отчетную дату dd.mm.yyyy: "), "%d.%m.%Y")
wDate_minus1 = wDate + timedelta(days=-1)
wDateStr = "'" + str(wDate.strftime("%Y-%m-%d")) + "'"
wDate_minus1Str = "'" + str(wDate_minus1.strftime("%Y-%m-%d")) + "'"
# wDate = datetime.strptime(input("Введите отчетную дату"), "%d.%m.%Y")
conn = psycopg2.connect(database=db_name, host=host, user=user, password=password)
conn.autocommit = True
cursor = conn.cursor()

# очистка транзитной таблицы
cursor.execute("TRUNCATE TABLE msp_transit")
# импорт файла csv в транзитную таблицу базы
sql = '''COPY msp_transit 
    FROM '/tmp/msp.csv'
    DELIMITER ','
    CSV HEADER'''
cursor.execute(sql)
>>>>>>>>> Temporary merge branch 2

# Заливка данных в основную таблицу
# 1. Обновление данных по записям, которые отсутствуют в исходнике - исключены из реестра МСП
# 1.1. Создание таблицы с индексами отсутствующих предприятий
try:
      cursor.execute("DROP TABLE tab1")
except:
      pass
cursor.execute("CREATE TABLE tab1 ( pk varchar(15) )")
sql = "INSERT INTO tab1 (PK) " \
      "SELECT msp.ogrn_num AS PK FROM msp WHERE msp.ogrn_num NOT IN (SELECT ogrn_num FROM msp_transit);"
cursor.execute(sql)
# 1.2. меняем даты окончания действия записи по отваливающимся предприятиям (отчетная минус 1)
sql = "UPDATE msp SET effective_to_dt = " + wDate_minus1Str + " " \
      "FROM tab1  " \
      " WHERE (msp.ogrn_num = tab1.pk) AND msp.is_active_flg = 1 and msp.effective_to_dt = '5999-12-31';"
# print(sql)
# cursor.execute(sql)
# cursor.execute("DROP TABLE tab1")
# 2. Обработка клиентов, по которым меняются реквизиты
# 2.1. ОБновление дат окончания действия по записям, которые будут актуализироваться
try:
      cursor.execute("DROP TABLE tab2")
except:
      pass
cursor.execute("CREATE TABLE tab2 ( pk varchar(15) )")
sql = '''INSERT INTO tab1 (pk) 
            SELECT tr.pk
            FROM (
                  SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm 
                  FROM msp 
                  WHERE is_active_flg = 1 
                  UNION
                  SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm 
                  FROM msp_transit) AS tr
            GROUP BY tr.pk
            HAVING count(tr.pk) > 1;
      '''
cursor.execute(sql)

# 2.2.
# 3. Добавление новых клиентов