import boto3
import logging
from string import Template
from typing import Union

from FunctionAnalytics.utils.table_template_report import TABLE_TEMPLATE
from FunctionAnalytics.utils.spacing_config_report import (SPACING_CONFIG, QTD_FIELDS, VALOR_FIELDS)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SnsTransactionReportSender:
    def __init__(self, topic_arn: str):
        self.topic_arn = topic_arn
        self.sns = boto3.client("sns")
        # já vem como Template diretamente
        self.template = Template(TABLE_TEMPLATE)

    def execute(self, rows: list[dict]):
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

    def build_subs(self, rows: list[dict], start_index: int = 1) -> dict:
        subs = {}
        for idx, row in enumerate(rows, start=start_index):
            cols = {
                'a': ("qtd", self.format_int),
                'b': ("valor", self.format_currency),
                'c': ("qtd_apr", self.format_int),
                'd': ("valor_apr", self.format_currency),
                'e': ("qtd_neg", self.format_int),
                'f': ("valor_neg", self.format_currency),
                'g': ("qtd_est", self.format_int),
                'h': ("qtd_adv", self.format_int),
                'i': ("tps", lambda x: str(x))
            }
            for col_key, (field, fmt) in cols.items():
                raw = fmt(row[field])

                if field in QTD_FIELDS:
                    cfg = SPACING_CONFIG["qtd"]
                elif field in VALOR_FIELDS:
                    cfg = SPACING_CONFIG["valor"]
                else:
                    cfg = None

                if cfg and (n_spaces := cfg.get(len(raw))) is not None:
                    padded = self.pad_center(raw, n_spaces, " ")
                else:
                    padded = raw

                subs[f"{col_key}{idx}"] = padded

        return subs

    @staticmethod
    def format_int(n: int) -> str:
        return f"{n:,}".replace(",", ".")

    @staticmethod
    def format_currency(v: Union[int, float, str]) -> str:
        try:
            cents = int(float(v))
        except (TypeError, ValueError):
            cents = 0

        valor_reais = cents / 100
        s = f"{valor_reais:,.2f}"  # "100,000,000.12"
        s = s.replace(",", "X")  # "100X000X000.12"
        s = s.replace(".", ",")  # "100X000X000X12,00"
        s = s.replace("X", ".")  # "100.000.000.12,00"

        return f"R$ {s}"

    @staticmethod
    def pad_center(raw: str, total_spaces: int, pad_char: str) -> str:
        left = total_spaces // 2 + (total_spaces % 2)
        right = total_spaces // 2
        return f"{pad_char * left}{raw}{pad_char * right}"
