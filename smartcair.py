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
sql_str = """SELECT
  periode, witel, snd, username_agent, nama_agent, r_caring_status, r_caring_voc, r_caring_tgl, r_caring_keterangan, r_caring_retensi, r_caring_retensi_ket, r_caring_kemauan, r_caring_kemampuan, TO_CHAR(tglpaid_inet1, 'YYYY-MM-DD') tglpaid_inet1, rp_paid_inet1, fungsi_agent, r_caring_tgl_jb
FROM
  TELKOMBDA..CHURN_SMARTCAIR_VOC_LAST
WHERE
  PERIODE = (SELECT max(periode) FROM TELKOMBDA..CHURN_SMARTCAIR_VOC_LAST)
  AND r_caring_status <> ''"""
curs = conn.cursor()
curs.execute(sql_str)
result = curs.fetchall()
# print(result[0])

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

trunc_ctc = cursor.execute("""truncate table public.ctc_data_from_smartcair""")
connection.commit()
for a in result:
    if a[16] == '':
        rct = '1970-01-01'
    else:
        rct = a[16]
    insert_ctc = """INSERT INTO ctc_data_from_smartcair (periode, witel, snd, username_agent, nama_agent, r_caring_status, r_caring_voc, r_caring_tgl, r_caring_keterangan, r_caring_retensi, r_caring_retensi_ket, r_caring_kemauan, r_caring_kemampuan, tglpaid_inet1, rp_paid_inet1, fungsi_agent, r_caring_tgl_jb)
VALUES(%s,%s,%s,%s,%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(insert_ctc,(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13], a[14], a[15], rct))
    connection.commit()

#update current
update_query = """UPDATE
	public.ctc_dapros_pranpc a
SET
	updated_at = now(),
	user_id = b.user_id,
	tgl_caring = b.tgl_caring,
	hcaring_id = b.hcaring_id,
	ofi_id = b.ofi_id,
	keterangan = b.keterangan,
	wo_status_id = b.wo_status_id,
	janji_bayar = b.janji_bayar,
	janji_bayar_ts = b.janji_bayar_ts,
	first_escalation_unit_id = b.first_escalation_unit_id
FROM
	(
	SELECT 
		x.*,
		y.escalation_id,
		CASE WHEN hcaring_id = 7 OR y.escalation_id IS NOT NULL THEN 2 ELSE NULL END wo_status_id,
		CASE WHEN hcaring_id = 7 THEN 6 ELSE y.escalation_id END first_escalation_unit_id
	FROM
		(
		SELECT
		    periode,
		    snd,
		    b.id user_id,
		    r_caring_tgl tgl_caring,
		    r_caring_status,
		    r_caring_voc,
		    CASE 
		        WHEN r_caring_status = 'NOT CONTACTED' AND r_caring_voc = 'VISIT-Rumah Tidak Berpenghuni' THEN 6
		        WHEN r_caring_status = 'NOT CONTACTED' AND r_caring_voc = 'VISIT-Alamat Tidak Ditemukan' THEN 7
		        WHEN r_caring_status = 'NOT CONTACTED' AND r_caring_voc = 'VISIT-Pindah Alamat' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Bukan Keluarga YBS' THEN 5
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Jarang Dipakai' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Pindah Kompetitor' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Sudah Ada Indihome Lain' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Sudah Minta Cabut' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Efisiensi' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Kemahalan' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Tagihan Melonjak' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Produk - Gangguan Layanan' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'CALL-BUSY' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'CALL-FAX' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'CALL-RNA' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'CALL-SALAH SAMBUNG' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'CALL-TIDAK BISA DIHUBUNGI' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'VISIT-Alamat Tidak Ditemukan' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'VISIT-Pindah Alamat' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'VISIT-Rumah Tidak Berpenghuni' THEN 4
		    ELSE
		        NULL
		    END hcaring_id,
		    CASE
		        WHEN r_caring_status = 'NOT CONTACTED' AND r_caring_voc = 'VISIT-Pindah Alamat' THEN 9
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Jarang Dipakai' THEN 11
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Pindah Kompetitor' THEN 6
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Sudah Ada Indihome Lain' THEN 8
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Efisiensi' THEN 5
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Kemahalan' THEN 4
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Price - Tagihan Melonjak' THEN 7
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Produk - Gangguan Layanan' THEN 1
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'VISIT-Pindah Alamat' THEN 9
		        WHEN r_caring_status = 'CONTACTED' AND r_caring_voc = 'Customer - Sudah Minta Cabut' THEN 10
		    ELSE 
		        NULL
		    END ofi_id,
		    r_caring_keterangan keterangan,
		    CASE WHEN r_caring_tgl_jb IS NULL THEN 0 ELSE 1 END janji_bayar,
		    CASE WHEN r_caring_tgl_jb IS NULL THEN NULL ELSE r_caring_tgl_jb END janji_bayar_ts
		FROM
		    public.ctc_data_from_smartcair a JOIN public.ctc_users b ON a.username_agent = b.username
		) x LEFT JOIN ctc_escalation_ofi y ON x.ofi_id = y.ofi_id
	) b
WHERE
	a.nper = b.periode AND a.nd = b.snd and a.ofi_id IS NULL AND coalesce(a.hcaring_id, 0) <> COALESCE(b.hcaring_id, 0)"""
cursor.execute(update_query)
rowupdated = cursor.rowcount
endtime = (time.time() - start_time)
connection.commit()

# # Send to Telegram
api_token = '2037558893:AAFgrAMVv7aUBUI_d8RYtkculNC6GWniklA'
bot = telegram.Bot(api_token)
notif = "Update from SMARTCAIR.  \n[{} rows updated {} seconds]".format(rowupdated,endtime)
bot.send_message(chat_id=83164754,text=notif)
# cursor.close()
# connection.close()
