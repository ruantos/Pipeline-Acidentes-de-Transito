-- Esse modelo acessa os dados na 'camada silver', extrai a hora da coluna horario
-- Para na query, agrupar os registros pela hora que ocorreram e contar o número total
-- de vítimas por cada grupo de hora
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
LIMIT 10