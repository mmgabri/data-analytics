# lambda_function.py

import logging
import json

from services.athena_report_data_provider_service import AthenaReportDataProvider
from services.sns_transaction_report_sender_service  import SnsTransactionReportSender
from application.use_case_transaction_report import UseCaseTransactionReport

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# configura o logger global (já no seu código)
import logging
root = logging.getLogger()
if not root.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(handler)
root.setLevel(logging.INFO)

# e então crie um logger para este módulo
logger = logging.getLogger(__name__)

# constantes de configuração
ATHENA_DB      = "autorizador_debito"
ATHENA_OUTPUT  = "s3://mmgabri-autorizador-debito/dados/queries-athena/"
TABLE_TEMPLATE = "table_template.txt"
TOPIC_ARN      = "arn:aws:sns:us-east-1:140023369634:send-email-analytics"

_provider = AthenaReportDataProvider(
    database       = ATHENA_DB,
    output_location= ATHENA_OUTPUT
)

# _sender = SnsEmailSender(
#     topic_arn     = TOPIC_ARN,
#     template_path = TABLE_TEMPLATE
# )

_sender = SnsTransactionReportSender(topic_arn=TOPIC_ARN)

_use_case = UseCaseTransactionReport(_provider, _sender)

def lambda_handler(event, context):
    logger.info("Lambda invocada")
    logger.debug(f"Evento recebido: {json.dumps(event)}")
    _use_case.execute()
