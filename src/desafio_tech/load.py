import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)
DB_PATH = "../../data/acidentes_de_transito.bd"
USED_COLS = ['hora', 'pedestre', 'ciclista', 'moto', 'tipo',
       'caminhao', 'auto', 'bairro',
       'onibus', 'data', 'vitimas',
       'endereco', 'viatura']


class Loader:
	def __init__(self, db_path: str) -> None:
		self.cursor = self.conn= None
		self.db_path = db_path

	def connect(self) -> None:
		try:
			self.conn = sqlite3.connect(self.db_path)
			self.cur = self.conn.cursor()
		except Exception as e:
			logging.info(f"Error caught while trying to connect to database {e}")


	def insert_bronze(self, df: pd.DataFrame) -> None:
		df = df[USED_COLS]

		self.create_bronze()
		df.to_sql("bronze_acidentes", self.conn, if_exists="append", index=False)


	def create_bronze(self):
		self.cur.execute("""
			CREATE TABLE IF NOT EXISTS bronze_acidentes (
			    id INTEGER PRIMARY KEY AUTOINCREMENT,
			    hora TEXT,
			    pedestre INTEGER,
			    ciclista INTEGER,
			    moto INTEGER,
			    tipo TEXT,
			    caminhao INTEGER,
			    auto INTEGER,
			    bairro TEXT NOT NULL,
			    onibus INTEGER,
			    data TEXT,
			    vitimas INTEGER,
			    endereco TEXT,
			    viatura TEXT
			);
			""")
		self.conn.commit()


	def close(self) -> None:
		self.conn.close()