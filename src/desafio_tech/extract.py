import requests
import pandas as pd
import logging


logger = logging.getLogger(__name__)
ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?sql="
QUERY = "SELECT * FROM "
DATASET = "http://dados.recife.pe.gov.br/api/3/action/package_show?id=acidentes-de-transito-com-e-sem-vitimas"
identifier = "d153f88e-3c25-422b-8b94-ef8d660bf7bf"


def get_resources_ids() -> list[str]:
	try:
		response = requests.get(url=DATASET, timeout=60)
		resources = response.json()['result']['resources']
		return [resources[i]['id'] for i in range( len(resources) )]

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch ids: {e}")
		return []


def fetch_dataframes(identifier: str) -> pd.DataFrame:
	query = f'SELECT * FROM "{identifier}"'
	url = f'{ENDPOINT}{query}'
	try:
		response = requests.get(url=url, timeout=60)
		records = response.json()["result"]["records"]
		if not records:
			logger.warning(f'No records found for dataset: {identifier}')
			return pd.DataFrame(records)

		return pd.DataFrame(records)



	except KeyError as e:
		logger.warning(f'No records found for dataset: {identifier}\nError: {e}')

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch dataframe: {e}")



if __name__ == "__main__":
	ids = get_resources_ids()
	for id in ids:
		print(type(fetch_dataframes(id)))