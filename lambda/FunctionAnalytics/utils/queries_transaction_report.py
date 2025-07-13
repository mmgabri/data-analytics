QUERY_PRESENTE_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM presente 
WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_PRESENTE_GET_ESTORNOS = """
SELECT bandeira,  tipo, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM presente 
WHERE dia_mes_ano = '{dia_mes_ano}' 
AND tipo = 'estorno'
GROUP BY bandeira, tipo
"""

QUERY_PRESENTE_GET_ADVICES = """
SELECT bandeira, tipo, COUNT() as quantidade, SUM(CAST(valor AS bigint)) AS valor_total 
FROM presente 
WHERE
reason_code in ('402', '120' ) 
AND tipo = 'advice' 
AND dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, tipo 
"""

QUERY_DIGITAIS_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM digitais 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_DIGITAIS_GET_ESTORNOS = """
SELECT bandeira,  tipo, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM digitais 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
AND tipo = 'estorno'
GROUP BY bandeira, tipo
"""

QUERY_DIGITAIS_GET_ADVICES = """
SELECT bandeira, tipo, COUNT() as quantidade, SUM(CAST(valor AS bigint)) AS valor_total 
FROM digitais 
WHERE
reason_code in ('402', '120' ) 
AND tipo = 'advice' 
AND dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, tipo 
"""

QUERY_LEGADO_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM legado 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_LEGADO_GET_ESTORNOS = """
SELECT bandeira,  tipo, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM legado 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
AND tipo = 'estorno'
GROUP BY bandeira, tipo
"""

QUERY_LEGADO_GET_ADVICES = """
SELECT bandeira, tipo, COUNT() as quantidade, SUM(CAST(valor AS bigint)) AS valor_total 
FROM legado 
WHERE
reason_code in ('402', '120' ) 
AND tipo = 'advice' 
AND dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, tipo 
"""

QUERY_PRESENTE_GET_PICO_TPS = """
SELECT substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM presente
QUERE
dia_mes_ano = '{dia_mes_ano}'
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1; 
"""