import cx_Oracle
import clickhouse_connect
import logging
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime as dt

load_dotenv(find_dotenv())

clickUser = os.environ.get("CLICKUSER")
clickPass = os.environ.get("CLICKPASS")
clickHost = os.environ.get("CLICKHOST")
clickDB = os.environ.get("CLICKDB")
clickTableV = os.environ.get("CLICKTABLEV")
clickTableN = os.environ.get("CLICKTABLEN")
oraUser = os.environ.get("ORAROUSER")
oraPass = os.environ.get("ORAROPASS")
oraHost = os.environ.get("ORAHOST")

logging.basicConfig(level=logging.INFO, filename="ora_cli.log", filemode="w", format="%(asctime)s %(levelname)s %(message)s")

try:
    connection = cx_Oracle.connect(oraUser + "/" + oraPass + oraHost, encoding="utf8", nencoding="utf8")
    logging.info(f"Connection to Oracle was succesdful.")
except Exception as e:
    logging.error("Exception", exc_info=True)

try:
    client = clickhouse_connect.get_client(host=clickHost, port=8443, username=clickUser, password=clickPass, database=clickDB, secure=True)
    logging.info(f"Connection to Clikhouse was succesdful.")
except Exception as e:
    logging.error("Exception", exc_info=True)


def rc_vll():
    cursor = connection.cursor()
    with open('sql/{0}.sql'.format(clickTableV), 'r') as f:
        cursor.execute(f.read())
    mon_rc_vll = cursor.fetchall()
    insert_to_click(clickTableV, mon_rc_vll)
    logging.info(f"Select from Oracle and insert to click table '{0}' was succesdful".format(clickTableV))


def rc_nday():
    cursor = connection.cursor()
    with open('sql/{0}.sql'.format(clickTableN), 'r') as f:
        cursor.execute(f.read())
    mon_rc_nday = cursor.fetchall()
    insert_to_click(clickTableN, mon_rc_nday)
    logging.info(f"Select from Oracle and insert to click table '{0}' was succesdful".format(clickTableN))


def insert_to_click(table, data):
    name_of_columns = client.command("SELECT name FROM system.columns WHERE database = '{0}' and table = '{1}'".format(clickDB,table))
    name_col = name_of_columns.split('\n')
    client.insert(table, data, column_names=name_col)

if __name__ == "__main__":
    try:
        rc_nday()
        rc_vll()
        logging.info(f"Program finished succesfully")
    except Exception as e:
        logging.error("Exception", exc_info=True)
