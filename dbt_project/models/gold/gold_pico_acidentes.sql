with gold_pico_acidentes AS (
    SELECT
        horario,
        EXTRACT(hour FROM horario) AS hora
    FROM {{ ref('silver_acidentes') }}
)

SELECT hora,
    COUNT(*) AS num_acidentes
FROM gold_pico_acidentes
GROUP BY hora
ORDER BY num_acidentes DESC