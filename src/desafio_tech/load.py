import duckdb
import logging
import pandas as pd

logger = logging.getLogger(__name__)
USED_COLS = ['_id', 'hora', 'pedestre', 'ciclista', 'moto', 'tipo',
       'caminhao', 'auto', 'bairro',
       'onibus', 'data', 'vitimas',
       'endereco', 'viatura']


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


	def insert_bronze(self, df: pd.DataFrame) -> None:
		"""
		Inserir os registros do daframe passado como parâmetro à tabela bronze_acidentes
		Algumas colunas são filtradas para padronização dos múltiplos recursos
		:param df:
		:return:
		"""
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
		"""
		Cria a tabela bronze_acidentes no banco duckdb
		:return:
		"""
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
		""" Encerra a conexão com o banco duckdb"""
		if self.conn:
			self.conn.close()
			logger.info("Closed duckdb connection successfully!")