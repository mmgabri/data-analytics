QUERY_PRESENTE_TOP_10_ERROS = """
SELECT bandeira, codigo_retorno, desc_erro, count(*) as quantidade
FROM (
    SELECT bandeira, codigo_retorno, desc_erro
FROM presente
WHERE dia_mes_ano = '{dia_mes_ano}' and codigo_retorno != '00'
UNION ALL
SELECT bandeira, codigo_retorno, desc_erro
FROM digitais
WHERE dia_mes_ano = '{dia_mes_ano}' and codigo_retorno != '00'
UNION ALL
SELECT bandeira, codigo_retorno, desc_erro
FROM legado
WHERE dia_mes_ano = '{dia_mes_ano}' and codigo_retorno != '00'
) AS todas_transacoes
GROUP BY bandeira, codigo_retorno, desc_erro
ORDER BY quantidade DESC
LIMIT 10;
"""


QUERY_PRESENTE_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM presente 
WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_PRESENTE_GET_ESTORNOS = """
SELECT bandeira, COUNT() as quantidade 
FROM presente 
WHERE dia_mes_ano = '{dia_mes_ano}' 
AND tipo = 'estorno'
GROUP BY bandeira
"""

QUERY_PRESENTE_GET_ADVICES = """
SELECT COUNT() as quantidade  
FROM presente 
WHERE
reason_code in ('402', '120' ) 
AND tipo = 'advice' 
AND dia_mes_ano = '{dia_mes_ano}' 
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

QUERY_PRESENTE_GET_PICO_TPS_GERAL = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}'
		 UNION ALL
		SELECT data_hora
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}'		
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_MODERNIZADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}'
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_LEGADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}'
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'		
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_MODERNIZADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_LEGADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'MASTERCARD'
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_VISA = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'
		 UNION ALL
		SELECT data_hora
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'		
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_VISA_MODERNIZADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_VISA_LEGADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}' and bandeira = 'VISA'
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_PRESENTE = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM presente
    WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_DIGITAIS = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM digitais
    WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_LEGADO = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""