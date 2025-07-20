import logging
from string import Template
from typing import Union
import re

import boto3

from FunctionAnalytics.utils.spacing_config_report import (SPACING_CONFIG, QTD_FIELDS, VALOR_FIELDS, PERCENT)
from FunctionAnalytics.utils.transaction_report_template import TRANSACTION_REPORT_TEMPLATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SnsTransactionReportSender:
    def __init__(self, topic_arn: str):
        self.topic_arn = topic_arn
        self.sns = boto3.client("sns")
        self.template = Template(TRANSACTION_REPORT_TEMPLATE)

    def execute(self, transaction_data):
        logger.info("Montando corpo do email…")

        rows = transaction_data.get("rows", [])
        top_10_erro = transaction_data.get("top_10_erro", [])
        date_base = transaction_data.get("date_base", [])

        subs = self.build_subs(rows, top_10_erro, date_base)
        body = self.template.substitute(subs)
        body = self.clean_empty_pipe_lines(body)

        logger.info("Enviando pelo SNS…")
        self.sns.publish(
            TopicArn=self.topic_arn,
            Message=body,
            Subject='[AUTORIZADOR DÉBITO] Relatório Transacional diário',
            MessageStructure='raw'
        )

        logger.info("Email enviado com sucesso!")

    def build_subs(self, rows: list[dict], top_10_erro: dict[str, list[dict]], date_base: str, start_index: int = 1) -> dict[str, str]:
        subs: dict[str, str] = {}

        # DATA BASE
        subs["date"] = date_base

        # BIG NUMBERS
        for idx, row in enumerate(rows, start=start_index):
            cols = {
                'a': ("percent", lambda x: str(x)),
                'b': ("qtd", self.format_int),
                'c': ("valor", self.format_currency),
                'd': ("qtd_apr", self.format_int),
                'e': ("valor_apr", self.format_currency),
                'f': ("qtd_neg", self.format_int),
                'g': ("valor_neg", self.format_currency),
                'h': ("tps", lambda x: str(x))
            }
            for col_key, (field, fmt) in cols.items():
                raw = fmt(row[field])

                if field in QTD_FIELDS:
                    cfg = SPACING_CONFIG["qtd"]
                elif field in VALOR_FIELDS:
                    cfg = SPACING_CONFIG["valor"]
                elif field in PERCENT:
                    cfg = SPACING_CONFIG["percent"]
                else:
                    cfg = None

                if cfg and (n_spaces := cfg.get(len(raw))) is not None:
                    padded = self.pad_center(raw, n_spaces, " ")
                else:
                    padded = raw

                subs[f"{col_key}{idx}"] = padded

        # TOP 10 NEGADAS - MASTERCARD
        self._fill_top10(subs, top_10_erro.get("MASTERCARD", []), start_idx=50)

        # TOP 10 NEGADAS - VISA
        self._fill_top10(subs, top_10_erro.get("VISA", []), start_idx=60)

        return subs

    def _fill_top10(
            self,
            subs: dict[str, str],
            top10_list: list[dict],
            start_idx: int,
            max_items: int = 10
    ):
        # limpa toda a faixa aX, bX, cX
        for offset in range(max_items):
            idx = start_idx + offset
            subs[f"a{idx}"] = ""
            subs[f"b{idx}"] = ""
            subs[f"c{idx}"] = ""

        # preenche só até len(top10_list)
        for offset, err in enumerate(top10_list[:max_items]):
            idx = start_idx + offset
            subs[f"a{idx}"] = err["cod_ret"]

            raw_qtd = self.format_int(err["qtd"])
            cfg = SPACING_CONFIG["qtd"]
            if cfg and (n_spaces := cfg.get(len(raw_qtd))) is not None:
                subs[f"b{idx}"] = self.pad_center(raw_qtd, n_spaces, " ")
            else:
                subs[f"b{idx}"] = raw_qtd

            subs[f"c{idx}"] = err["desc"]


    @staticmethod
    def format_int(n) -> str:
        if n == '-':
            return '-'
        try:
            num = int(n)
        except (TypeError, ValueError):
            return str(n)
        return f"{num:,}".replace(",", ".")


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
        if raw == '-':
            total_spaces2 = total_spaces + 1
        else:
            total_spaces2 = total_spaces

        left = total_spaces2 // 2 + (total_spaces2 % 2)
        right = total_spaces2 // 2
        return f"{pad_char * left}{raw}{pad_char * right}"

    def clean_empty_pipe_lines(self, rendered_text: str) -> str:
        """
        Substitui '|' por ' ' em qualquer linha que seja composta apenas por
        pipes e espaços (ex: '    |    |   ').
        """
        linhas = rendered_text.splitlines()
        novas = []
        for linha in linhas:
            # detecta linhas que têm só espaços e pipes
            if re.fullmatch(r'[ \t]*\|[ \t]*\|[ \t]*', linha):
                # troca todos os '|' por espaço
                linha = linha.replace('|', ' ')
            novas.append(linha)
        return "\n".join(novas)
