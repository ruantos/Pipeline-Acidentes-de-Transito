import requests
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASTORE_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql"
DATASET_ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/package_show?id=acidentes-de-transito-com-e-sem-vitimas"


def get_ids() -> list[str]:
	try:
		logger.info("Fetching resources id's...")
		response = requests.get(url=DATASET_ENDPOINT, timeout=60)
		resources = response.json()['result']['resources']
		logger.info("ID's acquired successfully\n")
		return [res['id'] for res in resources if res['datastore_active']]

	except requests.exceptions.RequestException as e:
		logger.error(f"An error occurred while trying to fetch ids: {e}")
		return []


def fetch_dataframe(identifier: str) -> pd.DataFrame:
	params = {
		'sql': f' SELECT * FROM "{identifier}" '
	}

	try:
		logger.info(f"Fetching {identifier} dataframe")
		response = requests.get(url=DATASTORE_ENDPOINT, params=params, timeout=60)
		response.raise_for_status()
		records = response.json()["result"]["records"]
		if not records:
			logger.info(f'No records found for dataset: {identifier}')

		logger.info(f"Dataframe fetched successfully")

		df = pd.DataFrame(records)
		return normalize_cols(df)

	except KeyError as e:
		logger.error(f'No records found for dataset: {identifier}\nError: {e}')
		return pd.DataFrame()

	except requests.exceptions.RequestException as e:
		logger.error(f"An error ({response.status_code}) occurred while trying to fetch dataframe: {e}")
		return pd.DataFrame()


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
	df.columns = [col.lower() for col in df.columns]
	year = df['data'].iloc[0].split("-")[0]
	df["_id"] = df["_id"].astype(str) + year + '0'
	return df