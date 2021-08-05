#!/usr/bin/python3

import configparser
import MySQLdb 					# Для работы с MySQL
import re 				        # Работаем с регулярками
import os 
import cx_Oracle  			        # Подключение к БД
import pandas as pd                             # работа с таблицами
import pretty_html_table                        # перевод DataFrame в HTML
import smtplib                                  # работа с SMTP сервером
from email.mime.multipart import MIMEMultipart  # создаём сообщение
from email.mime.text import MIMEText            # вёрстка письма
from datetime import date
from datetime import timedelta
from datetime import datetime
import calendar

config = configparser.ConfigParser()
config.read('config.ini', encoding='utf-8-sig')

server = config.get('mail', 'server')
From = config.get('mail', 'From')
To = config.get('mail', 'To')
path_to_files =config.get('DB', 'path_to_sql_file')
ora_server = config.get('DB', 'ora_server')
ora_login = config.get('DB', 'ora_login')
ora_pass = config.get('DB', 'ora_pass')

# Вычесляем текущую дату, дату начала и дату окончания
today = date.today()
first_of_month = today.replace(day=1)
last_of_month = first_of_month - timedelta(days=1)
start_date = ('01.'+last_of_month.strftime("%m.%Y")+' 00:00:00')
end_date = (last_of_month.strftime("%d.%m.%Y")+' 23:59:59')

#start_date = '01.02.2021 00:00:00'
#end_date = '28.02.2021 23:59:59'

input_date = {
	'date1': start_date,
	'date2': end_date
	}

# Решение пробелмы с кодировкой из-за наличия руссикх символов в SQL запросе, в том числе даже в коментариях
os.environ["NLS_LANG"] = ".AL32UTF8" 

# Присоединение и выполнение SQL запроса к БД Oracle
connection = cx_Oracle.connect(ora_login, ora_pass, ora_server)
print("Database version:", connection.version)
print("Encoding:", connection.encoding)

cursor = connection.cursor()
query_from_file = open(path_to_files+'sql_interconnection_traffic_rtk_mtt.sql')
sql_query = query_from_file.read()
df = pd.read_sql(sql_query, params = input_date, con=connection)


# Обработка ошибок. Если нет данных в выборке SQL, то выдаёся сообщение начинающееся с Empty. Регулярками находим это. Если данные есть, то 
# выдаётся сообщение None. Рабочим оказался вариант сравнивать именно с None, т.к. если наоборот, то проблема с типами данных (не разобрался)
# Если обработку не делать, то тоже сваливается в ошибку из за отсутствия данных в выборке

result = re.match(r'Empty', str(df))
if str(result) == 'None':
    html_table = pretty_html_table.build_table(df, 'blue_light', 'x-small')
else:
    html_table = 'Нет данных'

# подключаемся к SMTP серверу
server = smtplib.SMTP(server)
#server.login('email_login', 'email_password')
 
# создаём письмо
msg = MIMEMultipart('mixed')
msg['Subject'] = 'Трафик по договорам присоединения'
msg['From'] = From
msg['To'] = To
       
#добавляем в письмо текст и таблицу
html_table = MIMEText('<h2>Трафик за период с '+start_date+' по '+end_date+' </h2>'+html_table, 'html')
 
msg.attach(html_table)
 
# отправляем письмо
server.send_message(msg)
 
# отключаемся от SMTP сервера
server.quit()




