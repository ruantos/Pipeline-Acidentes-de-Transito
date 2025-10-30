# Desafio Tech

pipeline de dados para análise de acidentes de trânsito na RMR e demais municípios pernambucanos.


## Navegação
- **Relatórios:** ./reports/relatorio.md
- **Tratamento:** ./dbt_project/models
- **Consultas:** ./dbt_project/models/gold/*
- **Script de extração e carregamento:** ./src/desafio-tech/script.py
- 

## Descrição

Este projeto implementa um pipeline de dados que extrai, carrega e transforma dados de acidentes de trânsito. A transformação dos dados é feita pelo dbt, seguindo uma arquitetura de medalhão (bronze, silver, gold).

## Requisitos

- Python 3.11 ou superior
- Poetry
## Instalação

1.  Clone o repositório:
    ```bash
    git clone <url-do-repositorio>
    cd desafio-tech
    ```
2.  Instale as dependências do projeto com o Poetry:
    ```bash
    poetry install
    ```
3.  Configure o perfil do dbt. Crie o arquivo `profiles.yml` no diretório `~/.dbt/` com o seguinte conteúdo:
    ```yaml
    dbt_project:
      target: dev
      outputs:
        dev:
          type: duckdb
          path: data/acidentes_de_transito.bd
    ```
    **Observação:** O caminho (`path`) para o banco de dados DuckDB é relativo à raiz do projeto.

## Executando o Projeto

1.  **Extração e Carga (ETL):** Execute o script principal para extrair os dados da fonte e carregá-los na camada bronze do data warehouse (DuckDB).
    ```bash
    poetry run python src/desafio_tech/script.py
    ```
2.  **Transformação dos Dados:** Execute os modelos do dbt para transformar os dados e criar as camadas silver e gold.
    ```bash
    poetry run dbt run
    ```
3.  **Visualizando os Resultados:** Você pode visualizar um preview dos dados dos modelos materializados. Por exemplo, para ver os dados do modelo `gold_pico_acidentes`:
    ```bash
    poetry run dbt show --select gold_pico_acidentes
    ```

    ```bash
    poetry run dbt show --select gold_acidentes_ano
    ```