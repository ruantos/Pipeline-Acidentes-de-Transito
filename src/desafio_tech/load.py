import psycopg
import pandas as pd


def filters_columns(df: pd.DataFrame) -> pd.DataFrame:
	used_cols = ['natureza', 'tipo', 'vitimas', 'hora', 'data', 'bairro',
	             'ciclista', 'moto','caminhao', 'auto', 'pedestre','onibus',
                 'endereco', 'viatura']

	return df[used_cols]


def load(df: pd.Dataframe):
	df = filters_columns(df)

