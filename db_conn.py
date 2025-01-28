import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

load_dotenv(Path(f"{os.getcwd()}/.env"), override=True, verbose=True)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            port=DB_PORT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
        )
        print("STATUS: DB connection(1) succeed")
        return conn
    except Exception as e1:
        print("ERROR (1): ", e1)
        print("ERROR: DB connection(1) failed")


def query_db(query: str):
    """
    Use for CREATE, INSERT syntax
    """
    try:
        conn = connect_db()
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.commit()
        print("STATUS: Query succeed")

    except Exception as e2:
        conn.rollback()
        print("ERROR (2):", e2)
        print("ERROR: Query failed!")


def connect_db_v2():
    try:
        database = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        print(database)
        engine = create_engine(database)
        print("STATUS: DB connection(2) succeed")
        return engine
    except:
        print("ERROR: DB connection(2) failed")


def fetch_db(query: str):
    """
    Use for SELECT syntax
    """
    try:
        engine = connect_db()
        return pd.read_sql_query(query, con=engine)
    except Exception as e3:
        print("ERROR (3): ", e3)


if "__main__" == __name__:
    connect_db()
    connect_db_v2()
