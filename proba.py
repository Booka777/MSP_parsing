import pandas as pd
from datetime import date, datetime, timedelta

# #create DataFrame
# df = pd.DataFrame({'points': [25, 12, 15, 14, 19, 23],
#  'assists': [5, 7, 7, 9, 12, 9],
#  'rebounds': [11, 8, 10, 6, 6, 5]})
#
# #view DataFrame
# print(df)
# df.to_csv (r'/home/an/Загрузки/MSP/my_data.csv', index= False )
# df.to_csv (r'/home/an/Загрузки/MSP/my_data1.csv')

wDate = datetime.strptime(input("Введите отчетную дату"), "%d.%m.%Y")
wDateStr = wDate.strftime("%Y-%m-%d")
print(wDate)
print(wDate + timedelta(days=-1))
print(wDateStr)