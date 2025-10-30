# Relatório - Desafio de Engenharia de Dados

## Dados

### Descrição dos Dados
- Nese desafio foi utilizado o dataset **"Chamados de Sinistros (Acidentes) de Trânsito com e sem vitimas 2015 a 2024"**
- Os dados estão disponíveis no Portal de Dados Abertos da Prefeitura de Recife. 
- São coletados e disponibilizados na plataforma pela CTTU.
- A base é atualizada anualmentem, e possui registros desde 2015 à 2024 

### Fonte dos Dados
Os dados são públicos e foram acessados através da API da Dados Recife.

*   **Link para o dataset na plataforma:** [Chamados de Sinistros (Acidentes) de Trânsito com e sem vitimas 2015 a 2024](http://dados.recife.pe.gov.br/dataset/acidentes-de-transito-com-e-sem-vitimas)
*   **Ferramenta de extração:** As biblioteca `requests e pandas` foram utilizadas para consultar e 'baixar' os dados diretamente em formato de tabular.

### Motivação
A escolha deste dataset se deu por alguns motivos:
- facilidade de acesso: Aleḿ de ser de fácil manejo, já havia trabalhado com a plataforma e sua API anteriormente e já estava habituado com ela
- relevância dos dados: Além de serem dados reais, referem-se a problemas concretos da RMR (Região Metropolitana de Recife), com isso sua análise pode gerar diversas soluções para a região
- clubismo: Não nego, por ser Pernambucano gosto bastante de trabalhar com dados locais :)

## Extração e Transformação

### Visão Geral do Projeto
O projeto foi estruturado com **Poetry** para gerenciamento de dependências e organização em módulos, seguindo um modelo ELT (Extract, Load, Transform):

1.  **Extração (Extract):**
   - O módulo `extract.py` utiliza a biblioteca `requests` para se conectar à plataforma e requisitar o id dos recursos (tabela) desse dataset.
   - Ao requisitar todos ids ativos, é feita a requisição de cada set pelo seu id na API
   - Retorna cada recurso como um DataFrame da biblioteca `pandas`
2.  **Carregamento (Load):** 
   - O módulo `load.py` é responsável por criar a classe Loader que irá fazer o carregamento de todas as tabelas no banco
   - A classe Loader irá, respectivamente, conectar-se à um banco `Duckdb`, Criar uma tabela a partir do DataFrame passado como argumento e inserir seus dados na respectiva.
   - O banco, caso não exista, será gerado no caminho `./data/acidentes_de_transito.bd`
3.  **Transformação (Transform):**
   - Aqui o `dbt` é utilizado para transformar os dados que foram armazenados 'crus' no banco de dados.
   - Buscou-se organizar os dados em uma arquitetura similar a 'medalhão', com camads bronze, silver e gold.
   - A camada bronze representa os dados inalterados no banco de dados. Inserídos da forma que foram extraídos
   - O modelo na camada silver (silver_acidentes.sql) é responsável por limpar, padronizar (nomes e datatypes) e concatenar os dados de acidentes
     de trânsito de múltiplos anos em uma tabela.
     Os modelos na camada gold (gold_acidentes_ano.sql e gold_pico_acidentes.sql) agregam os dados da camada silver para
     gerar insights e possibilitar futuras análises, como o total de vítimas por ano e os horários de pico de acidentes.


## Consultas SQL

As consultas foram escritas para explorar os dados de Sinistros registrados na RMR e gerar insights.

  Consulta 1 (`gold_acidentes_ano`): Atualização Anual do número de Vítimas de Acidentes
   * Descrição: Esta consulta gold_acidentes_ano, calcula o número total de vítimas de acidentes de trânsito para cada ano e a média mensal de vítimas.
   * Motivação: Possibilita analisar a tendência de longo prazo no número de vítimas, permitindo avaliar a eficácia de políticas públicas

  Consulta 2 (`gold_pico_acidentes`): Distribuição de acidentes por hora do dia
   * Descrição: A consulta identifica os horários do dia com a maior concentração de
     acidentes, contando o número de ocorrências para cada hora.
   * Motivação: Identificar os períodos mais críticos do dia para a ocorrência de acidentes. Essa informação é fundamental
     para otimizar o policiamento, direcionar campanhas de conscientização para os horários de maior risco e planejar
     operações de fiscalização.
   * 

## LOG messages
Ao longo de todo o projeto foi utilizado a biblioteca nativa do Python `Logging` para exibição de erros e feedbacks durante a execução.

## Reflexões
O projeto foi uma ótima oportunidade para exercitar conceitos e ferramentas, como ELT e dbt. O primeiro, já me era conhecido,
mas sempre foquei em projetos de ETL. O segundo, o dbt, já era uma ferramenta que eu planejo dar mais atenção, visto ser orientada a SQL 