# lambda_function.py

import logging
import json

from FunctionAnalytics.services.data_provider import AthenaDataProvider
from FunctionAnalytics.services.email_sender  import SnsEmailSender
from FunctionAnalytics.application.use_case_relatorio import RelatorioUseCase

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# constantes de configuração
ATHENA_DB      = "autorizador_debito"
ATHENA_OUTPUT  = "s3://mmgabri-autorizador-debito/dados/queries-athena/"
#TABLE_TEMPLATE = "/var/task/table_template.txt"
TABLE_TEMPLATE = "table_template.txt"
TOPIC_ARN      = "arn:aws:sns:us-east-1:140023369634:notification-agilfacil"

_provider = AthenaDataProvider(
    database       = ATHENA_DB,
    output_location= ATHENA_OUTPUT
)

_sender = SnsEmailSender(
    topic_arn     = TOPIC_ARN,
    template_path = TABLE_TEMPLATE
)

_use_case = RelatorioUseCase(_provider, _sender)

def lambda_handler(event, context):
    logger.info("Lambda invocada")
    logger.debug(f"Evento recebido: {json.dumps(event)}")
    _use_case.execute()
