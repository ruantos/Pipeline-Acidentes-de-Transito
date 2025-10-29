from pathlib import Path
from load import Loader
from extract import get_ids, fetch_dataframe


DB_PATH = Path(__file__).parent.parent.parent / "data" / "acidentes_de_transito.bd"


if __name__ == "__main__":
	ids = get_ids()
	loader = Loader(DB_PATH)

	for identifier in ids:
		df = fetch_dataframe(identifier)
		loader.insert_bronze(df)
	loader.close()
