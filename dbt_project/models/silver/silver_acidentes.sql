-- Esse modelo padroniza e atribui os datatypes corretos às colunas das
-- tabelas raw no duckdb e concatena-as (as tabelas) em uma só

-- gravidade: nova coluna criada para possível análise da gravidade da ocorrência
-- à partir da quantidade de feridos envolvidos
WITH bronze_acidentes_2024 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2024') }}

),

bronze_acidentes_2023 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2023') }}

),

bronze_acidentes_2022 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2022') }}

),

bronze_acidentes_2021 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2021') }}

),

bronze_acidentes_2020 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2020') }}

),

bronze_acidentes_2019 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2019') }}

),

bronze_acidentes_2018 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2018') }}

),

bronze_acidentes_2017 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2017') }}

),

bronze_acidentes_2016 AS (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2016') }}

),

bronze_acidentes_2015 as (
    SELECT
        _id,
        tipo,
        CAST(REPLACE(vitimas, ',', '.') AS INTEGER) AS vitimas,
        CASE
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) = 0 THEN 'leve'
            WHEN CAST(REPLACE(vitimas, ',', '.') AS INTEGER) < 3 THEN 'moderado'
            ELSE 'grave'
        END AS gravidade,
        TRY_CAST(hora AS TIME) AS horario,
        CAST(SUBSTR(data, 1, 10) AS DATE) AS data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        CAST(REPLACE(auto, ',', '.') AS INTEGER) AS auto,
        CAST(REPLACE(moto, ',', '.') AS INTEGER) AS moto,
        CAST(REPLACE(caminhao, ',', '.') AS INTEGER) AS caminhao,
        CAST(REPLACE(onibus, ',', '.') AS INTEGER) AS onibus,
        CAST(REPLACE(ciclista, ',', '.') AS INTEGER) AS ciclista,
        CAST(REPLACE(pedestre, ',', '.') AS INTEGER) AS pedestre
    FROM {{ source('bronze', 'bronze_acidentes_2015') }}

)

SELECT * FROM bronze_acidentes_2024
UNION ALL
SELECT * FROM bronze_acidentes_2023
UNION ALL
SELECT * FROM bronze_acidentes_2022
UNION ALL
SELECT * FROM bronze_acidentes_2021
UNION ALL
SELECT * FROM bronze_acidentes_2020
UNION ALL
SELECT * FROM bronze_acidentes_2019
UNION ALL
SELECT * FROM bronze_acidentes_2018
UNION ALL
SELECT * FROM bronze_acidentes_2017
UNION ALL
SELECT * FROM bronze_acidentes_2016
UNION ALL
SELECT * FROM bronze_acidentes_2015