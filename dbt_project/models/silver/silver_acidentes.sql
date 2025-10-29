with bronze_acidentes_2024 as (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2024') }}

),

bronze_acidentes_2023 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2023') }}

),

bronze_acidentes_2022 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2022') }}

),

bronze_acidentes_2021 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2021') }}

),

bronze_acidentes_2020 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2020') }}

),

bronze_acidentes_2019 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2019') }}

),

bronze_acidentes_2018 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2018') }}

),

bronze_acidentes_2017 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2017') }}

),

bronze_acidentes_2016 AS (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2016') }}

),

bronze_acidentes_2015 as (
    SELECT
        _id,
        tipo,
        vitimas,
        hora,
        data,
        bairro,
        NULL::VARCHAR AS tempo_clima,
        auto,
        moto,
        caminhao,
        onibus,
        viatura,
        ciclista,
        pedestre
    FROM {{ ref('bronze_acidentes_2015') }}

),

select * from bronze_acidentes_2024
union all
select * from bronze_acidentes_2023
union all
select * from bronze_acidentes_2022
union all
select * from bronze_acidentes_2021
union all
select * from bronze_acidentes_2020
union all
select * from bronze_acidentes_2019
union all
select * from bronze_acidentes_2018
union all
select * from bronze_acidentes_2017
union all
select * from bronze_acidentes_2016
union all
select * from bronze_acidentes_2015
