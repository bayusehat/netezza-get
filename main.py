import jaydebeapi
import os
import psycopg2
from psycopg2 import Error
import telegram
import time
from datetime import datetime, timedelta
ystrday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
now = (datetime.now()).strftime('%Y%m%d')
# import ctypes
start_time = time.time()
# Netezza Get
db = "TELKOMPG"
host = "10.62.187.9"
port = "5480"
user = "USR_PROBIS"
password = "usr#probis"
jdbc_driver_name = "org.netezza.Driver"
jdbc_driver_loc = os.path.join('/xampp4/htdocs/netezza-get/nzjdbc.jar')

connection_string = 'jdbc:netezza://'+host+':'+port+'/'+db
url = '{0}:user={1};password={2}'.format(connection_string, user, password)

conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': user, 'password': password},
                          jars=jdbc_driver_loc)

# Run SQL Netezza
sql_str = """SELECT nper, nd, is_paid FROM telkombda..CHURN_SYMPTOM_GROUP WHERE nper = '202212' AND is_paid = 1 AND to_char(PAYMENT_DATE, 'YYYYMMDD') IN ('{}', '{}')""".format(ystrday,now)
curs = conn.cursor()
curs.execute(sql_str)
result = curs.fetchall()
jumlah =  len(result);
# Postgre In
try:
    connection = psycopg2.connect(user="ccoper", password="ccoper2019", host="10.60.170.169", port="5432",
                                  database="ccoper", keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=2)
    cursor = connection.cursor()
    print("PostgreSQL server information")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    print("PostgreSQL connection is closed")

trunc_ctc = cursor.execute("""truncate table public.ctc_current_update_paid""")
connection.commit()
for a in result:
    insert_ctc = """insert into public.ctc_current_update_paid (nper, nd, is_paid) values ('{}','{}','{}')""".format(
        a[0], a[1], a[2])
    cursor.execute(insert_ctc)
    connection.commit()

#update current
update_query = """UPDATE public.ctc_dapros_current a SET is_paid = b.is_paid, updated_at = now() FROM public.ctc_current_update_paid b WHERE 1 = 1 AND a.nd = b.nd AND b.is_paid = 1 AND a.is_paid = 0"""
cursor.execute(update_query)
rowupdated = cursor.rowcount
endtime = (time.time() - start_time)
connection.commit()

# # Send to Telegram
api_token = '2037558893:AAFgrAMVv7aUBUI_d8RYtkculNC6GWniklA'
bot = telegram.Bot(api_token)
notif = "Update paid CURRENT.  \n[{} rows updated {} seconds]".format(rowupdated,endtime)
bot.send_message(chat_id=83164754,text=notif)
# bot.send_message(chat_id=-83164754,text=notif);
# cursor.close()
# connection.close()
