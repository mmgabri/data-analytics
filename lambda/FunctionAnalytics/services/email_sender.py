# email_sender.py

import boto3
import logging
from string import Template
from pathlib import Path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SnsEmailSender:
    def __init__(self, topic_arn: str, template_path: str = "table_template.txt"):    
        self.topic_arn = topic_arn
        self.sns = boto3.client("sns")
        pkg_root      = Path(__file__).parent.parent
        template_path = pkg_root / template_path
        # monta o path para FunctionAnalytics/table_template.txt
        raw = template_path.read_text(encoding="utf-8")
        self.template = Template(raw)

    @staticmethod
    def format_int(n: int) -> str:
        return f"{n:,}".replace(",", ".")

    @staticmethod
    def format_currency(v: float) -> str:
        s = f"{v:,.2f}"
        return "R$ " + s.replace(",", "X").replace(".", ",").replace("X", ".")

    def build_subs(self, rows: list[dict], start_index: int = 2) -> dict:
        subs = {}
        for idx, row in enumerate(rows, start=start_index):
            subs[f"b{idx}"] = self.format_int(row["qtd"])
            subs[f"c{idx}"] = self.format_currency(row["valor"])
            subs[f"d{idx}"] = self.format_int(row["qtd_neg"])
            subs[f"e{idx}"] = self.format_currency(row["valor_neg"])
            subs[f"f{idx}"] = self.format_int(row["qtd_apr"])
            subs[f"g{idx}"] = self.format_currency(row["valor_apr"])
            subs[f"h{idx}"] = self.format_int(row["qtd_est"])
            subs[f"i{idx}"] = self.format_int(row["qtd_adv"])
            subs[f"j{idx}"] = str(row["tps"])
        return subs

    def send(self, rows: list[dict]):
        logger.info("Montando corpo do email…")
        subs = self.build_subs(rows)
        body = self.template.substitute(subs)
        logger.info("Enviando pelo SNS…")
        self.sns.publish(
            TopicArn=self.topic_arn,
            Message=body,
            Subject='Relatório Transacional',
            MessageStructure='raw'
        )
        logger.info("Email enviado com sucesso!")
