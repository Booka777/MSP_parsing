import pandas as pd

path_file = r'/home/an/Загрузки/MSP/msp.csv'
wFile = pd.read_csv(path_file,nrows=100)
# print(wFile.head(10))
print(wFile.to_csv(None))

print(wFile.info())
#