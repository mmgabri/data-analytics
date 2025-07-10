import boto3
import logging
from string import Template
from pathlib import Path
from typing import Union  

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

QTD_FIELDS = {"qtd", "qtd_neg", "qtd_apr", "qtd_est", "qtd_adv"}
VALOR_FIELDS = {"valor", "valor_neg", "valor_apr"}

SPACING_CONFIG = {
    "qtd": {
        1: 23,
        2: 21,
        3: 19,
        5: 16,
        6: 13,
        7: 11,
        9: 8,
       10: 6,
       11: 3,
    },
    "valor": {
        7: 25,
        8: 22,
        9: 20,
       11: 16,
       12: 14,
       13: 12,
       15: 8,
       16: 6,
       17: 4,
    },
}

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
        s = f"{reais:,.2f}"            # "100,000,000.12"
        s = s.replace(",", "X")        # "100X000X000X12"
        s = s.replace(".", ",")        # "100X000X000X12,00"  
        s = s.replace("X", ".")        # "100.000.000.12,00"

        return f"R$ {s}"

    @staticmethod
    def pad_center(raw: str, total_spaces: int, pad_char: str) -> str:
        left = total_spaces // 2 + (total_spaces % 2)
        right = total_spaces // 2
        return f"{pad_char * left}{raw}{pad_char * right}"
     
    def build_subs(self, rows: list[dict], start_index: int = 2) -> dict:
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
