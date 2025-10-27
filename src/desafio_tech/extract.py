import requests
import pandas as pd
import logging


logger = logging.getLogger(__name__)
ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?"
ENDPOINT_LIST = "http://dados.recife.pe.gov.br/api/3/action/package_list"
identifier = "d153f88e-3c25-422b-8b94-ef8d660bf7bf"


def get_sets_names() -> list[str] | None:
	word = 'acidentes'
	try:
		response = requests.get(url=ENDPOINT_LIST, timeout=60)
		datastore = response.json()["result"]

		if not datastore:
			logger.warning(f'No datasets found in the datastore for phrase: {word}')
			return []
		return [dataset for dataset in datastore if dataset.startswith(word)]

	except requests.exceptions.RequestException as e:
		logger.warning(f"An error occurred while trying to fetch dataframe: {e}")




def fetch_dataframes(identifier: str) -> pd.DataFrame:
	query = 'sql=SELECT * FROM '
	url_params = f'{ENDPOINT}{query}"{identifier}"'

	try:
		response = requests.get(url_params, timeout=60)
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
	pass