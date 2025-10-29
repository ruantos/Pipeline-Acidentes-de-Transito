-- Esse modelo acessa os dados na 'camada silver', exrai o ano da coluna data,
-- agrupa os registros pela mesma coluna e calcula o total de vítimas de acidentes
-- por ano.
-- Na querye seguinte usa-se a coluna extraída 'ano' para calcular também a média
-- de acidentes por mês
with gold_acidentes_ano AS (
    SELECT EXTRACT(year FROM data ) AS ano,
           CAST( SUM(vitimas) AS INTEGER) AS total_vitimas,
    FROM {{ ref('silver_acidentes') }}
    GROUP BY ano
    ORDER BY total_vitimas DESC

)

SELECT ano,
       total_vitimas,
       (total_vitimas / 12) AS "media_vitimas_mes"
FROM gold_acidentes_ano