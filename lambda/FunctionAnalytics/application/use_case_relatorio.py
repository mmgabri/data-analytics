# use_case_relatorio.py

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class RelatorioUseCase:
    def __init__(self, data_provider, email_sender):
        self.data_provider = data_provider
        self.email_sender = email_sender

    def execute(self):
        logger.info("UseCaseRelatorio: iniciando execução…")
        data = self.data_provider.get_data()
        rows = data.get("rows", [])
        if not rows:
            logger.warning("Nenhuma linha retornada pelo provider!")
        self.email_sender.send(rows)
        logger.info("UseCaseRelatorio: finalizado.")