# Парсинг реестров МСП из XML файлов и сохранение в csv
import os
import xml.etree.ElementTree as ET
import pandas as pd


def getFromXML(xml_file):
    tree = ET.ElementTree(file=xml_file)
    root = tree.getroot()
    # print(root)
    # print("tag=%s, attrib=%s" % (root.tag, root.attrib))
    # print(root.attrib["ВерсФорм"])

    total_list = []
    for child in root: #Документ
        client = []
        if child.tag =="Документ":
            # client.append(child.attrib['ИдДок'])
            # client.append(child.attrib['ДатаСост'])
            client.append(child.attrib['ДатаВклМСП'])
            client.append(child.attrib['ВидСубМСП'])
            client.append(child.attrib['КатСубМСП'])
            client.append(child.attrib['ПризНовМСП'])
            client.append(child.attrib['СведСоцПред'])
            client.append(child.get('ССЧР'))
            for sub_child in child:    # реквизиты клиента
                if  sub_child.tag == "ОргВклМСП":
                    client.append(sub_child.get('ИННЮЛ'))
                    client.append(sub_child.get('ОГРН'))
                    client.append(sub_child.get('НаимОргСокр'))
                elif sub_child.tag == "ИПВклМСП":
                    client.append(sub_child.get('ИННФЛ'))
                    client.append(sub_child.get('ОГРНИП'))
                    fio = ''
                    try: fio += ' ' + sub_child[0].attrib['Фамилия']
                    except: fio = fio
                    try: fio += ' ' + sub_child[0].attrib['Имя']
                    except: fio = fio
                    try: fio += ' ' + sub_child[0].attrib['Отчество']
                    except: fio = fio
                    client.append(fio)

            total_list.append(client)
    return total_list

path_to_XML = "/home/an/dl/MSP/XML/"
# path_to_XML = "C:/_STORE/MSP/xml/"
files_list = os.listdir(path_to_XML)

tList = []
count = 0
for file in files_list:
    tList.append(getFromXML(path_to_XML + file))
    print(count)
    count += 1
newlist = []
for i in tList:
    for j in i:
        newlist.append(j)
df = pd.DataFrame(newlist,
                  columns=['ДатаВклМСП', 'ВидСубМСП', 'КатСубМСП', 'ПризНовМСП', 'СведСоцПред', 'ССЧР', 'ИНН', 'ОГРН',
                           'Наименование']
                  )
# df.to_csv('C:/_STORE/MSP/msp.csv')
df.to_csv('/home/an/dl/MSP/msp.csv',index=False)
# df = pd.DataFrame(newlist)      #если добавить .T после скобки, таблица транспонируется
# df.to_excel(excel_writer='C:/_STORE/MSP/msp.xlsx')
# df.to_excel(excel_writer='/home/an/Загрузки/МСП/msp.xlsx')

