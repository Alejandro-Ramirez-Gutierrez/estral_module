# config.py
import pyodbc
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()  # Carga variables del .env

# ---------------- SQL Server ----------------
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
)

def get_connection():
    """Conexión a SQL Server"""
    return pyodbc.connect(CONN_STR)

# ---------------- MySQL ----------------
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")

def get_mysql_connection():
    """Conexión a MySQL"""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS
    )
