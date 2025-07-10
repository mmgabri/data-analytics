import boto3
import logging
from string import Template
from pathlib import Path
from typing import Union  

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# largura máxima de cada coluna (sem contar os dois espaços de padding do template)
COLUMN_WIDTHS = {
    'b': 9,   # QTD
    'c': 16,   # Valor
    'd': 9,   # QTD NEG.
    'e': 16,   # Valor NEG.
    'f': 9,   # QTD APR.
    'g': 16,   # Valor APR.
    'h': 9,   # QTD EST.
    'i': 9,   # QTD Advice
    'j': 3     # TPS
}
FIGURE_SPACE = "\u2007"

class SnsEmailSender:
    def __init__(self, topic_arn: str, template_path: str = "table_template.txt"):    
        self.topic_arn = topic_arn
        self.sns = boto3.client("sns")
        pkg_root      = Path(__file__).parent.parent
        template_path = pkg_root / template_path
        raw = template_path.read_text(encoding="utf-8")
        self.template = Template(raw)

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

    @staticmethod
    def format_int(n: int) -> str:
        return f"{n:,}".replace(",", ".")

    @staticmethod
    def format_currency(v: Union[int, float, str]) -> str:
        try:
            cents = int(float(v))
        except (TypeError, ValueError):
            cents = 0

        reais = cents / 100
        s = f"{reais:,.2f}"            # ex: "100,000,000.12"
        s = s.replace(",", "X")        # "100X000X000X12"
        s = s.replace(".", ",")        # "100X000X000X12,00"  (aqui o ponto original vira vírgula do decimal)
        s = s.replace("X", ".")        # "100.000.000.12,00"

        return f"R$ {s}"

    def build_subs2(self, rows: list[dict], start_index: int = 2) -> dict:
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
    
    def build_subs(self, rows: list[dict], start_index: int = 2) -> dict:
    #    self.test_spacing_chars(self)
        subs = {}
        for idx, row in enumerate(rows, start=start_index):
            cols = {
                'b': ("qtd",       self.format_int),
                'c': ("valor",     self.format_currency),
                'd': ("qtd_neg",   self.format_int),
                'e': ("valor_neg", self.format_currency),
                'f': ("qtd_apr",   self.format_int),
                'g': ("valor_apr", self.format_currency),
                'h': ("qtd_est",   self.format_int),
                'i': ("qtd_adv",   self.format_int),
                'j': ("tps",       lambda x: str(x))
            }
            for col_key, (field, fmt) in cols.items():
                raw = fmt(row[field])
                logger.info(field) #qtd/valor/qtd_neg....
                logger.info(raw) #conteudo
                logger.info("raw='%s' tem %d caracteres", raw, len(raw))
                width = COLUMN_WIDTHS[col_key]
                padded = raw.rjust(width, FIGURE_SPACE)
                subs[f"{col_key}{idx}"] = padded
        return subs  