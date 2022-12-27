import psycopg2
from config import host, user, password, db_name

conn = psycopg2.connect(database=db_name, host=host, user=user, password=password)
conn.autocommit = True
cursor = conn.cursor()

sql = "CREATE TABLE IF NOT EXISTS msp_transit (msp_adding_dt date, msp_type_flg smallint, msp_category_flg smallint, " \
      "new_msp_flg smallint, social_company_flg smallint, employees_cnt integer, inn_num varchar(12), " \
      "ogrn_num varchar(15), client_nm varchar(150) )"
cursor.execute(sql)

sql = "CREATE TABLE IF NOT EXISTS msp (id serial PRIMARY KEY, msp_adding_dt date, msp_type_flg smallint, msp_category_flg smallint, " \
      "new_msp_flg smallint, social_company_flg smallint, employees_cnt integer, inn_num varchar(12), " \
      "ogrn_num varchar(15), client_nm varchar(150) )"
cursor.execute(sql)

cursor.close()
conn.close()