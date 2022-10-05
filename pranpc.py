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
sql_str = "SELECT nper, nd, is_paid FROM telkombda..CHURN_SYMPTOM_GROUP_NPC WHERE is_paid = 0"
curs = conn.cursor()
curs.execute(sql_str)
result = curs.fetchall()

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

trunc_ctc = cursor.execute("""truncate table public.ctc_pranpc_update_paid""")
connection.commit()
for a in result:
    insert_ctc = """insert into public.ctc_pranpc_update_paid (nper, nd, is_paid) values ('{}','{}','{}')""".format(
        a[0], a[1], a[2])
    cursor.execute(insert_ctc)
    connection.commit()

# cursor.close()
# connection.close()
