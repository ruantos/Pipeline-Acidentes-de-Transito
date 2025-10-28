import requests
import pandas as pd
import logging
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASTORE_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?sql="
DATASET_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/package_show?id=acidentes-de-transito-com-e-sem-vitimas"


def get_ids() -> list[str]:
	try:
		logger.info("Fetching resources id's...")
		response = requests.get(url=DATASET_ENDPOINT, timeout=60)
		resources = response.json()['result']['resources']
		logger.info("ID's acquired successfully")
		return [res['id'] for res in resources if res['datastore_active']]

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch ids: {e}")
		return []


def fetch_dataframe(identifier: str) -> pd.DataFrame:
	query = f'SELECT * FROM "{identifier}"'
	url = f'{DATASTORE_ENDPOINT}{query}'

	try:
		logger.info(f"Fetching {identifier} dataframe")
		response = requests.get(url=url, timeout=60)
		records = response.json()["result"]["records"]
		if not records:
			logger.info(f'No records found for dataset: {identifier}')

		logger.info(f"Dataframe fetched successfully\n")
		return pd.DataFrame(records)

	except KeyError as e:
		logger.warning(f'No records found for dataset: {identifier}\nError: {e}')
		return pd.DataFrame()

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch dataframe: {e}")
		return pd.DataFrame()


def save_dataframe(df: pd.DataFrame):
	path = "../../data/bronze/"
	os.makedirs(path, exist_ok=True)

	try:

		df.columns = [column.lower() for column in df.columns]
		if 'data' not in df.columns:
			raise ValueError("Dataframe must not have the column DATA or have a empty column ")

		year = str(df['data'].iloc[0].split("-")[0])
		file_path = os.path.join(path, f"acidentes_de_transito_{year}.parquet")

		df.to_parquet(path=file_path, engine="pyarrow")
		logger.info(f"Dataframe saved successufuly at {file_path}")

	except Exception as e:
		logger.warning(f"An error occurred while trying to save the dataframe: {e}")


if __name__ == "__main__":
	ids = get_ids()
	dataframes = [fetch_dataframe(identifier) for identifier in ids]
	for df in dataframes:
		save_dataframe(df)


