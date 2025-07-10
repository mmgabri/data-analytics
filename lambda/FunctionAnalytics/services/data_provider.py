# data_provider.py

import boto3
import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

QUERY1 = "select bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, count() as quantidade from presente where data = '2025-07-05' group by status, bandeira"

class AthenaDataProvider:
    def __init__(self, database: str, output_location: str, athena_client=None):
        self.database = database
        self.output_location = output_location
        self.athena = athena_client or boto3.client('athena')

    def get_data(self) -> dict:
        logger.info("Executando query no Athena…")
        result_response = self.execute_query(QUERY1)
        logger.info(result_response)
        
        return {
        "rows": [
            {
                "name": "GERAL", "qtd": 67, "valor": 100012,
                "qtd_neg":123, "valor_neg":1000034,
                "qtd_apr":99, "valor_apr":12345678900,
                "qtd_est":1, "qtd_adv":0,
                "tps":300
            },
            {
                "name": "MODERNIZADO", "qtd": 99999,  "valor":  5500000032,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "LEGADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "MASTERCARD", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "VISA", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "MASTERCARD - MODERNIZADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "MASTERCARD - LEGADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "VISA - MODERNIZADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "VISA - LEGADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "AUTORIZADOR DIGITAL", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "AUTORIZADOR PRESENTE", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            },
            {
                "name": "AUTORIZADOR LEGADO", "qtd": 5000000,  "valor":  5500000000,
                "qtd_neg":5000000,  "valor_neg": 4000000000,
                "qtd_apr":4000000,  "valor_apr": 1230000000,
                "qtd_est":1000000,  "qtd_adv": 500000,
                "tps":150
            }
        ]
    }

    def execute_query(self, query) -> Dict[str, Any]:
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.output_location},
        )

        execution_id = response['QueryExecutionId']

        while True:
            result = self.athena.get_query_execution(QueryExecutionId=execution_id)
            status = result['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1)

        if status != 'SUCCEEDED':
            raise Exception(f"Query falhou: {status}")

        result_response = self.athena.get_query_results(QueryExecutionId=execution_id)
        return result_response
