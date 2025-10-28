import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)
USED_COLS = ['hora', 'pedestre', 'ciclista', 'moto', 'tipo',
       'caminhao', 'auto', 'bairro',
       'onibus', 'data', 'vitimas',
       'endereco', 'viatura']


class Loader:
	def __init__(self, db_path: str) -> None:
		logger.info(f"Connecting to database..")
		try:
			self.conn = sqlite3.connect(db_path)
			self.cur = self.conn.cursor()
			logger.info("Connection was successful")
		except Exception as e:
			logger.info(f"Error caught while trying to connect to database {e}")


	def insert_bronze(self, df: pd.DataFrame) -> None:
		df = df[USED_COLS]

		if not self.conn:
			logger.warning("No db connection founded!")
		else:
			try:
				self.create_bronze()
				df.to_sql("bronze_acidentes", self.conn, if_exists="append", index=False)
			except KeyError as e:
				logger.warning(f"Key Error caught while inserting records in Bronze: {e}")
			except Exception as e:
				logger.warning(f"Error caught while inserting records in Bronze: {e}")

	def create_bronze(self):
		if not self.conn:
			logger.warning("No db connection founded!")
		else:
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