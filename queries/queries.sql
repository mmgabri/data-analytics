--LINHA 1

-- por autorizador (presente + digitais + legado)
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025'		
    ) AS todas_transacoes
    
-- por autorizador (presente + digitais + legado) - status
SELECT status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT status,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT status,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT status,
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
GROUP BY status

-- por autorizador (presente + digitais + legado) - estornos
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
    ) AS todas_transacoes

-- por autorizador (presente + digitais + legado) - advices
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
    ) AS todas_transacoes









--LINHA 2

-- por autorizador (presente + digitais)
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
    
-- por autorizador (presente + digitais) - status
SELECT status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT status,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT status,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
GROUP BY status

-- por autorizador (presente + digitais) - estornos
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
    ) AS todas_transacoes

-- por autorizador (presente + digitais) - advices
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
    ) AS todas_transacoes





--LINHA 3

-- por autorizador (legado)
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
WHERE dia_mes_ano = '05-07-2025'

-- por autorizador ( legado) - status
SELECT status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
WHERE dia_mes_ano = '05-07-2025'
GROUP BY status

-- por autorizador (presente + digitais) - estornos
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado
WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'

-- por autorizador (legado) - advices
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )




--LINHA 4 e 7

-- por autorizador (presente + digitais + legado) bandeira
SELECT  bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT  bandeira,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira,
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025'		
    ) AS todas_transacoes
group by bandeira    
    
-- por autorizador (presente + digitais + legado) - bandeira/status
SELECT bandeira, status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, status,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira, status,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira, status,
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
GROUP BY bandeira, status

-- por autorizador e bandeira (presente + digitais + legado) - estornos
SELECT bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT  bandeira,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT bandeira,
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
    ) AS todas_transacoes
    group by bandeira

-- por autorizador (presente + digitais + legado) - advices
SELECT bandeira, 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM legado
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
    ) AS todas_transacoes
group by bandeira    





--LINHA 5 e 8

-- por autorizador (presente + digitais) bandeira
SELECT  bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT  bandeira,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
group by bandeira    
    
-- por autorizador (presente + digitais ) - bandeira/status
SELECT bandeira, status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, status,
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025'
		UNION ALL
		SELECT bandeira, status,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025'
    ) AS todas_transacoes
GROUP BY bandeira, status

-- por autorizador e bandeira (presente + digitais) - estornos
SELECT bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
		UNION ALL
		SELECT  bandeira,
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
    ) AS todas_transacoes
    group by bandeira

-- por autorizador (presente + digitais) - advices
SELECT bandeira, 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM (
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM presente
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
		UNION ALL
		SELECT bandeira, 
			valor,
			dia_mes_ano
		FROM digitais
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
    ) AS todas_transacoes
group by bandeira    




--LINHA 6 e 9

-- por autorizador (legado) bandeira
SELECT  bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado
		WHERE dia_mes_ano = '05-07-2025'
group by bandeira    
    
-- por autorizador (legado ) - bandeira/status
SELECT bandeira, status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
		WHERE dia_mes_ano = '05-07-2025'
GROUP BY bandeira, status

-- por autorizador e bandeira (legado) - estornos
SELECT bandeira,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'
group by bandeira

-- por autorizador (legado) - advices
SELECT bandeira, 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM legado 
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )
group by bandeira    




--LINHA 10, 11 e 12

-- por autorizador
SELECT  
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM presente
		WHERE dia_mes_ano = '05-07-2025'

    
-- por autorizador (legado ) - bandeira/status
SELECT status,
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM presente 
		WHERE dia_mes_ano = '05-07-2025'
GROUP BY status

-- por autorizador e bandeira (legado) - estornos
SELECT 
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM presente 
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'estorno'


-- por autorizador (legado) - advices
SELECT
	SUM(CAST(valor AS bigint)) AS valor_total,
	COUNT(*) AS quantidade
FROM presente 
		WHERE dia_mes_ano = '05-07-2025' and tipo = 'advice' and reason_code in ('402', '120' )



