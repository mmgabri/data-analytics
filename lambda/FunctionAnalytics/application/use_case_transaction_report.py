import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class UseCaseTransactionReport:
    def __init__(self, data_provider, email_sender):
        self.data_provider = data_provider
        self.email_sender = email_sender

    def execute(self):
        logger.info("UseCaseRelatorio: iniciando execução…")

        yesterday = date.today() - timedelta(days=1)
        date_formatted = yesterday.strftime("%m-%d-%Y")
        logger.info(yesterday)
        #data = self.data_provider.execute(date_formatted)
        transaction_data = self.data_provider.execute('05-07-2025')

        rows = transaction_data.get("rows", [])
        if not rows:
            logger.warning("Nenhuma linha retornada pelo provider!")

        self.email_sender.execute(rows)
        logger.info("UseCaseTransactionReport finalizado com sucesso!")