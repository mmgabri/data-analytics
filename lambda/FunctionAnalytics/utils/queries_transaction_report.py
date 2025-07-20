QUERY_PRESENTE_TOP_10_ERROS = """
(
  SELECT bandeira, codigo_retorno, desc_erro, COUNT(*) AS quantidade
  FROM (
    SELECT bandeira, codigo_retorno, desc_erro
    FROM presente
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
    UNION ALL
    SELECT bandeira, codigo_retorno, desc_erro
    FROM digitais
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
    UNION ALL
    SELECT bandeira, codigo_retorno, desc_erro
    FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
  ) t
  WHERE bandeira = 'MASTERCARD'
  GROUP BY bandeira, codigo_retorno, desc_erro
  ORDER BY quantidade DESC
  LIMIT 10
)
UNION ALL
(
  SELECT bandeira, codigo_retorno, desc_erro, COUNT(*) AS quantidade
  FROM (
    SELECT bandeira, codigo_retorno, desc_erro
    FROM presente
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
    UNION ALL
    SELECT bandeira, codigo_retorno, desc_erro
    FROM digitais
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
    UNION ALL
    SELECT bandeira, codigo_retorno, desc_erro
    FROM legado
    WHERE dia_mes_ano = '{dia_mes_ano}' AND codigo_retorno <> '00'
  ) t
  WHERE bandeira = 'VISA'
  GROUP BY bandeira, codigo_retorno, desc_erro
  ORDER BY quantidade DESC
  LIMIT 10
);
"""

QUERY_BY_PLATAFORMA = """
SELECT 
	 plataforma, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM (
		SELECT valor, plataforma, status
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}'
		 UNION ALL
		SELECT valor, plataforma, status
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' 
		 UNION ALL
		SELECT valor, plataforma, status
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}' 
    ) AS todas_transacoes
GROUP BY plataforma, status    
"""

QUERY_BY_TIPO_PESSOA = """
SELECT 
	 tipo_pessoa, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM (
		SELECT valor, tipo_pessoa, status
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}'
		 UNION ALL
		SELECT valor, tipo_pessoa, status
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' 
		 UNION ALL
		SELECT valor, tipo_pessoa, status
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}' 
    ) AS todas_transacoes
GROUP BY tipo_pessoa, status    
"""

QUERY_PRESENTE_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM presente 
WHERE dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_DIGITAIS_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM digitais 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
"""

QUERY_LEGADO_BY_STATUS = """
SELECT bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, COUNT() as quantidade 
FROM legado 
WHERE 
dia_mes_ano = '{dia_mes_ano}' 
GROUP BY bandeira, status
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

QUERY_PRESENTE_GET_PICO_TPS_SINGLE = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and plataforma = 'SINGLE_MESSAGE' AND bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and plataforma = 'SINGLE_MESSAGE' AND bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM legado
		 WHERE dia_mes_ano = '{dia_mes_ano}' and plataforma = 'SINGLE_MESSAGE' AND bandeira = 'MASTERCARD'	 
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""

QUERY_PRESENTE_GET_PICO_TPS_DUAL = """
SELECT 
	substring(data_hora, 1, 19) AS data_hora, COUNT(*) AS pico_tps
FROM (
		SELECT data_hora
 		 FROM presente
		 WHERE dia_mes_ano = '{dia_mes_ano}' and plataforma = 'DUAL_MESSAGE' AND bandeira = 'MASTERCARD'
		 UNION ALL
		SELECT data_hora
		 FROM digitais
		 WHERE dia_mes_ano = '{dia_mes_ano}' and plataforma = 'DUAL_MESSAGE' AND bandeira = 'MASTERCARD'
    ) AS todas_transacoes
GROUP BY substring(data_hora, 1, 19)
ORDER BY pico_tps DESC
LIMIT 1;   
"""