import logging
import psycopg2
from telegram.ext import Updater, InlineQueryHandler, CommandHandler, MessageHandler, Filters
from psycopg2 import Error
from datetime import datetime
import requests
import re

#Connecting database
try:
    connection = psycopg2.connect(user="ccoper",password="ccoper2019",host="10.60.170.169",port="5432",database="ccoper",keepalives=1,keepalives_idle=5,keepalives_interval=2,keepalives_count=2)
    cursor = connection.cursor()
    print("PostgreSQL server information")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    print("PostgreSQL connection is closed")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)

def echo(update, context):
    rep = update.message.reply_to_message
    if rep is None:
        return context.bot.send_message(chat_id=-530820999,text="Pesan tanpa Reply!")
    else:
        message_id = rep.message_id
        chat_id = rep.chat_id
        user = rep.text
        numbers = []
        for msg in user.split():
            if msg.isdigit():
                numbers.append(int(msg))
        nd = max(numbers)
        username = rep.from_user.username
        first_name = rep.from_user.first_name
        last_name =rep.from_user.last_name
        name = "{} {}".format(first_name,last_name)
        date = rep.date
        tipe = rep.chat.type
        is_reply = 1

        #Self Value Message
        response = update.message.text
        user_reply = update.message.from_user.username
        
        #Fetching Data from Database
        select_query = "select message_id from bot_rekap_main where message_id = '%s'"
        sc = connection.cursor()
        sc.execute(select_query,(message_id,))
        row = sc.fetchall()
        rowcount = len(row)
        if rowcount > 0 :
            if row[0][0] == message_id:
                return context.bot.send_message(chat_id=-530820999,text="Pesan ini sudah di reply, failed insert to Database!")

    pesan = "{}".format(user)
    insert_query = """insert into bot_rekap_main(nama,username,message,type,tgl,is_reply,message_id,nd,response,user_reply) 
        values('{}','{}','{}','{}','{}',{},{},'{}','{}','{}')""".format(name,username,pesan,tipe,date,is_reply,message_id,nd,response,user_reply)
    cursor.execute(insert_query)
    connection.commit()
    notif = "Stored in Database,successfully! \n[Pesan dari : {} ; Text : {} ; Tanggal : {} ] group id = {} ".format(name,pesan,date,chat_id)
    return context.bot.send_message(chat_id=-530820999,text=notif)


def main():
    updater = Updater('1894704804:AAGa7tvFU-WyJyAWWhz6ho_cuudcbQe7OdA',use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler('done',echo))
    dp.add_handler(MessageHandler(Filters.regex('done') 
    ^ Filters.regex('Done') 
    ^ Filters.regex('DONE')
    ^ Filters.regex('sesuai')
    ^ Filters.regex('Sesuai')
    ^ Filters.regex('SESUAI')
    ^ Filters.regex('sudah')
    ^ Filters.regex('Sudah')
    ^ Filters.regex('SUDAH')
    ^ Filters.regex('DOne')
    ,echo))
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()