# config.py
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()  # Carga variables del .env

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
    return pyodbc.connect(CONN_STR)

