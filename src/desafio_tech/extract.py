import requests
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASTORE_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?sql="
DATASET_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/package_show?id=acidentes-de-transito-com-e-sem-vitimas"


def get_ids() -> list[str]:
	try:
		logger.info("Fetching resources id's...")
		response = requests.get(url=DATASET_ENDPOINT, timeout=60)
		resources = response.json()['result']['resources']
		logger.info("ID's acquired successfully\n")
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

		logger.info(f"Dataframe fetched successfully")

		df = pd.DataFrame(records)
		return normalize_names(df)

	except KeyError as e:
		logger.warning(f'No records found for dataset: {identifier}\nError: {e}')
		return pd.DataFrame()

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch dataframe: {e}")
		return pd.DataFrame()


def normalize_names(df: pd.DataFrame) -> pd.DataFrame:
	df.columns = [col.lower() for col in df.columns]
	return df