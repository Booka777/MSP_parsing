import pandas as pd
import psycopg2
from config import host, user, password, db_name
from datetime import datetime, timedelta

def recCount (cursor, table):
      # sql1 = "SELECT count(*) FROM %s;"
      sql1 = "SELECT count(*) FROM " + table + ";"
      cursor.execute(sql1, (table,))
      mobile_records = cursor.fetchall()
      for row in mobile_records:
            count_ot_records = row[0]
      return count_ot_records

# ---------------это пробный блок -----------------
# path_file = r'/home/an/dl/MSP/msp.csv'
# wFile = pd.read_csv(path_file,nrows=100)
# # print(wFile.head(10))     # выводит заголовок таблицы
# print(wFile.to_csv(None))   # выводит полную таблицу
# print(wFile.info())         # информация по полям
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
print('Идет импорт файла csv в транзитную таблицу базы')
sql = '''COPY msp_transit 
    FROM '/tmp/msp.csv'
    DELIMITER ','
    CSV HEADER'''
cursor.execute(sql)


# Заливка данных в основную таблицу
# 1. Обновление данных по записям, которые отсутствуют в исходнике - исключены из реестра МСП
# 1.1. Создание таблицы с индексами отсутствующих предприятий
try:
      cursor.execute("DROP TABLE tab1")
except:
      pass
print('1.1. Создание таблицы с индексами отсутствующих предприятий')
cursor.execute("CREATE TABLE tab1 ( pk varchar(15) )")
# sql = "INSERT INTO tab1 (pk) " \
#       "SELECT msp.ogrn_num AS pk FROM msp WHERE msp.ogrn_num NOT IN (SELECT ogrn_num FROM msp_transit);"
sql = '''INSERT INTO tab1 (pk) 
            SELECT msp.ogrn_num 
            FROM msp LEFT JOIN msp_transit ON msp_transit.ogrn_num = msp.ogrn_num
            WHERE msp_transit.ogrn_num IS NULL AND msp.effective_to_dt = '5999-12-31';
      '''
cursor.execute(sql)
outer_count = recCount(cursor, 'tab1')
print('Отсутствующие:', outer_count)
# 1.2. Меняем даты окончания действия записи по отваливающимся предприятиям (отчетная минус 1)
print('1.2. Меняем даты окончания действия записи по отваливающимся предприятиям (отчетная минус 1)')
sql = "UPDATE msp SET effective_to_dt = {0} FROM tab1   WHERE (msp.ogrn_num = tab1.pk) AND msp.is_active_flg = 1 and msp.effective_to_dt = \'5999-12-31\';".format(wDate_minus1Str)
cursor.execute(sql)
# cursor.execute("DROP TABLE tab1")
print('Блок 1')
# 2. Обработка клиентов, по которым меняются реквизиты
# 2.1. Отбор кодов записей, которые будут актуализироваться (ключ - ОГРН)
print('2.1. Отбор кодов записей, которые будут актуализироваться (ключ - ОГРН)')
try:
      cursor.execute("DROP TABLE tab2")
except:
      pass
cursor.execute("CREATE TABLE tab2 ( pk varchar(15) )")
sql = '''INSERT INTO tab2 (pk) 
            SELECT tr.ogrn_num
            FROM (
                  SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm 
                  FROM msp 
                  WHERE effective_to_dt = '5999-12-31' 
                  UNION
                  SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm 
                  FROM msp_transit) AS tr
            GROUP BY tr.ogrn_num
            HAVING count(tr.ogrn_num) > 1;
      '''
cursor.execute(sql)

# 2.2. ОБновление дат окончания действия и флагов активности по записям, которые будут актуализироваться
print('2.2. ОБновление дат окончания действия и флагов активности по записям, которые будут актуализироваться')
sql = '''UPDATE msp SET effective_to_dt = ''' + wDate_minus1Str + ''', is_active_flg = 0 
      FROM tab2
      WHERE (msp.ogrn_num = tab2.pk) AND msp.is_active_flg = 1 and msp.effective_to_dt = '5999-12-31';
      '''
cursor.execute(sql)
# 2.3. Добавлкние актуализированных записей
change_count0 = recCount(cursor, 'msp')
sql = '''INSERT INTO msp (msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm, effective_from_dt, effective_to_dt, is_active_flg)
            SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm, ''' + wDateStr + ''' AS effective_from_dt, 
                  '5999-12-31' AS effective_to_dt, 1 AS is_active_flg
            FROM msp_transit, tab2
            WHERE (msp_transit.ogrn_num = tab2.pk) AND 1=1
      '''
cursor.execute(sql)
change_count = recCount(cursor,'msp') - change_count0
print('Обновленные:', change_count)
print('Блок 2')
# 3. Добавление новых клиентов
# 3.1. Отбор кодов ОГРН по клиентам, которые будут заливаться в базу впервые
print('3.1. Отбор кодов ОГРН по клиентам, которые будут заливаться в базу впервые')
try:
      cursor.execute("DROP TABLE tab2")
except:
      pass
cursor.execute("CREATE TABLE tab2 ( pk varchar(15) )")
# sql = "INSERT INTO tab2 (pk) " \
#       "SELECT msp_transit.ogrn_num AS pk FROM msp_transit WHERE msp_transit.ogrn_num NOT IN (SELECT ogrn_num FROM msp WHERE effective_to_dt = '5999-12-31' AND is_active_fkg = 1);"
sql = '''INSERT INTO tab2 (pk) 
            SELECT tr.ogrn_num 
            FROM msp_transit AS tr LEFT JOIN msp ON tr.ogrn_num = msp.ogrn_num
            WHERE msp.ogrn_num IS NULL;
      '''
cursor.execute(sql)
# 3.2. Заливка новых записей
print('3.2. Заливка новых записей')
add_count0 = recCount(cursor, 'msp')
sql = '''INSERT INTO msp (msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm, effective_from_dt, effective_to_dt, is_active_flg)
            SELECT msp_adding_dt, msp_type_flg, msp_category_flg, new_msp_flg, social_company_flg, employees_cnt, inn_num, ogrn_num, client_nm, msp_adding_dt AS effective_from_dt, 
                  '5999-12-31' AS effective_to_dt, 1 AS is_active_flg
            FROM msp_transit, tab2
            WHERE (msp_transit.ogrn_num = tab2.pk) AND 1=1
      '''
cursor.execute(sql)
add_count = recCount(cursor, 'msp') - add_count0
print('Исключены: ', outer_count)
print('Обновлены: ', change_count)
print('Добавлены: ', add_count)

if conn:
      cursor.close()
      conn.close()