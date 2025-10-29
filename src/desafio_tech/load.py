import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)
USED_COLS = ['_id', 'hora', 'pedestre', 'ciclista', 'moto', 'tipo',
       'caminhao', 'auto', 'bairro',
       'onibus', 'data', 'vitimas',
       'endereco', 'viatura']


class Loader:
	def __init__(self, db_path: str) -> None:
		logger.info(f"Connecting to duckdatabase..")
		try:
			self.conn = duckdb.connect(database=db_path)
			logger.info("Connection was successful")
		except Exception as e:
			logger.info(f"Error caught while trying to connect to database {e}")
			self.conn = None


	def insert_bronze(self, df: pd.DataFrame) -> None:
		if df.empty:
			return
		df = df[USED_COLS]
		year = str(df['data'].iloc[0].split("-")[0])

		if not self.conn:
			logger.warning("No db connection founded!")
		else:
			try:
				self.create_bronze()
				self.conn.execute("INSERT INTO bronze_acidentes SELECT * FROM df")
				logger.info(f"{year} Dataframe inserted successfully\n")

			except KeyError as e:
				logger.error(f"Key Error caught while inserting records in Bronze: {e}")

			except Exception as e:
				logger.error(f"Error caught while inserting records in Bronze: {e}")

	def create_bronze(self):
		if not self.conn:
			logger.warning("No db connection founded!")
		else:
			self.conn.execute("""
				CREATE TABLE IF NOT EXISTS bronze_acidentes (
				    _id VARCHAR(200),
				    hora VARCHAR(200),
				    pedestre VARCHAR(5),
				    ciclista VARCHAR(5),
				    moto VARCHAR(5),
				    tipo VARCHAR(200),
				    caminhao VARCHAR(5),
				    auto VARCHAR(5),
				    bairro VARCHAR(255),
				    onibus VARCHAR(5),
				    data VARCHAR(200),
				    vitimas VARCHAR(5),
				    endereco VARCHAR(255),
				    viatura VARCHAR(255)
				)
				""")


	def close(self) -> None:
		if self.conn:
			self.conn.close()
			logger.info("Closed duckdb connection successfully!")