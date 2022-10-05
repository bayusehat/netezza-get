import jaydebeapi
import os
import psycopg2
from psycopg2 import Error
# import ctypes

# Netezza Get
db = "xxxx"
host = "xxx"
port = "xxxx"
user = "xxxxx"
password = "xxxxx"
jdbc_driver_name = "org.netezza.Driver"
jdbc_driver_loc = os.path.join(
    '/home/telkom/Downloads/Telegram Desktop/nzjdbc.jar')

connection_string = 'jdbc:netezza://'+host+':'+port+'/'+db
url = '{0}:user={1};password={2}'.format(connection_string, user, password)

conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': user, 'password': password},
                          jars=jdbc_driver_loc)

# Run SQL Netezza
sql_str = "SELECT PERIODE,SND,USERNAME_AGENT,NAMA_AGENT,FUNGSI_AGENT,R_CARING_STATUS,R_CARING_VOC,R_CARING_TGL,R_CARING_TGL_JB,R_CARING_KETERANGAN,R_CARING_RETENSI,R_CARING_RETENSI_KET,R_CARING_KEMAUAN,R_CARING_KEMAMPUAN,TGLPAID_INET1,RP_PAID_INET1 FROM telkombda..CHURN_smartcair_voc_last WHERE r_caring_status <> ''"
curs = conn.cursor()
curs.execute(sql_str)
result = curs.fetchall()
print(result[0])

# Postgre In
try:
    connection = psycopg2.connect(user="xxxx", password="xxxxx", host="xxxx", port="5432",
                                  database="xxxxx", keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=2)
    cursor = connection.cursor()
    print("PostgreSQL server information")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    print("PostgreSQL connection is closed")

trunc_ctc = cursor.execute("""truncate table public.ctc_data_from_smartcair""")
connection.commit()
for a in result:
    insert_ctc = """insert into public.ctc_data_from_smartcair (PERIODE,SND,USERNAME_AGENT,NAMA_AGENT,FUNGSI_AGENT,R_CARING_STATUS,R_CARING_VOC,R_CARING_TGL,R_CARING_TGL_JB,R_CARING_KETERANGAN,R_CARING_RETENSI,R_CARING_RETENSI_KET,R_CARING_KEMAUAN,R_CARING_KEMAMPUAN,TGL_PAID_INET1,RP_PAID_INET1) values ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')""".format(
        a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15])
    cursor.execute(insert_ctc)
    connection.commit()

# cursor.close()
# connection.close()