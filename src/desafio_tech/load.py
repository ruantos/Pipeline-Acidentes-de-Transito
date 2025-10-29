import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class Loader:
	""" Classe responsável por se conectar ao banco, criar schemas e inserir os dados """

	def __init__(self, db_path: str) -> None:
		"""
		Construtor da classe Loader. Inicializa objeto e se conecta ao banco duckdb
		"""
		logger.info(f"Connecting to duckdatabase..")
		try:
			self.conn = duckdb.connect(database=db_path)
			logger.info("Connection was successful\n")
		except Exception as e:
			logger.info(f"Error caught while trying to connect to database {e}")
			self.conn = None


	def create_bronze(self, df: pd.DataFrame) -> None:
		"""
		Inserir os registros do daframe passado como parâmetro à tabela bronze_acidentes
		Algumas colunas são filtradas para padronização dos múltiplos recursos
		:param df:
		:return:
		"""
		if df.empty:
			return

		if not self.conn:
			logger.warning("No db connection founded!")
		else:
			try:
				year = self.get_year(df)
				self.conn.execute(F"CREATE TABLE bronze_acidentes_{year} AS SELECT * FROM df")
				logger.info(f"{year} Dataframe inserted successfully\n")

			except KeyError as e:
				logger.error(f"Key Error caught while inserting records in Bronze: {e}")

			except Exception as e:
				logger.error(f"Error caught while inserting records in Bronze: {e}")


	def get_year(self, df: pd.DataFrame) -> str:
		"""
		Retorna o ano ao qual o dafataframe se refere
		:param df:
		:return:
		"""
		return str(df['data'].iloc[0].split("-")[0])




	def close(self) -> None:
		""" Encerra a conexão com o banco duckdb"""
		if self.conn:
			self.conn.close()
			logger.info("Closed duckdb connection successfully!")