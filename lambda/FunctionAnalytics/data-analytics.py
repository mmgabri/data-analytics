import boto3
import time
import logging
import json
import datetime
import botocore.exceptions

ATHENA_DB = "autorizador_debito"
ATHENA_OUTPUT = "s3://mmgabri-autorizador-debito/dados/queries-athena/"
# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Lambda iniciada")
    logger.info(f"Evento recebido: {json.dumps(event)}")
    send_email(event)


def get_dados():
    client = boto3.client('athena')
    query = "select bandeira, status, SUM(CAST(valor AS bigint)) AS valor_total, count() as quantidade from presente where data = '2025-07-05' group by status, bandeira"

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    execution_id = response['QueryExecutionId']

    # Aguarda até a query completar
    while True:
        result = client.get_query_execution(QueryExecutionId=execution_id)
        status = result['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if status != 'SUCCEEDED':
        raise Exception(f"Query falhou: {status}")

    result_response = client.get_query_results(QueryExecutionId=execution_id)
    return result_response


def send_email(event):
    logger.info("Montando email....")
    topic_arn= 'arn:aws:sns:us-east-1:140023369634:notification-agilfacil'
    sns = boto3.client('sns')
    
    # Tabela ASCII em texto puro
    ascii_table = """\
    | Categoria                  | Quantidade | Valor | Quant. Negadas | Valor Negadas | Quant. Aprovadas | Valor Aprovadas | Quant. Estorno | Quant. Advice | Pico TPS |
    |----------------------------|------------|-------|----------------|---------------|------------------|-----------------|----------------|---------------|----------|
    | Geral                      |            |       |                |               |                  |                 |                |               |          |
    | Modernizado                |            |       |                |               |                  |                 |                |               |          |
    | Legado                     |            |       |                |               |                  |                 |                |               |          |
    | Mastercard                 |            |       |                |               |                  |                 |                |               |          |
    | Visa                       |            |       |                |               |                  |                 |                |               |          |
    | Mastercard – Modernizado   |            |       |                |               |                  |                 |                |               |          |
    | Mastercard – Legado        |            |       |                |               |                  |                 |                |               |          |
    | Visa – Modernizado         |            |       |                |               |                  |                 |                |               |          |
    | Visa – Legado              |            |       |                |               |                  |                 |                |               |          |
    | Autorizador Digital        |            |       |                |               |                  |                 |                |               |          |
    | Autorizador Presente       |            |       |                |               |                  |                 |                |               |          |
    | Autorizador Legado         |            |       |                |               |                  |                 |                |               |          |
    """

    # Publica como plain-text (sem JSON envelope)
    sns.publish(
        TopicArn=topic_arn,
        Message=ascii_table,
        Subject='Relatório Transações'
    )
    logger.info("Email enviado com sucesso....")   