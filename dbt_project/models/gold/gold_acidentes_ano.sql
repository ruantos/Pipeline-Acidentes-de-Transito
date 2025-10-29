with gold_acidentes_ano AS (
    SELECT EXTRACT(year FROM data ) AS ano,
           CAST( SUM(vitimas) AS INTEGER) AS total_vitimas
    FROM {{ ref('silver_acidentes') }}
    GROUP BY ano

)

SELECT *
FROM gold_acidentes_ano
ORDER BY total_vitimas DESC