import requests
import pandas as pd
import logging


logger = logging.getLogger(__name__)
ENDPOINT = "http://dados.recife.pe.gov.br/api/3/action/datastore_search_sql?"
identifier = "d153f88e-3c25-422b-8b94-ef8d660bf7bf"


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



