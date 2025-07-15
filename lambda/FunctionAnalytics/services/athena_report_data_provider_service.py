import logging
import time
from collections import defaultdict
from datetime import datetime

import boto3

from FunctionAnalytics.utils.queries_transaction_report import (
    QUERY_PRESENTE_BY_STATUS,
    QUERY_PRESENTE_GET_ESTORNOS,
    QUERY_PRESENTE_GET_ADVICES,
    QUERY_DIGITAIS_BY_STATUS,
    QUERY_DIGITAIS_GET_ESTORNOS,
    QUERY_DIGITAIS_GET_ADVICES,
    QUERY_LEGADO_BY_STATUS,
    QUERY_LEGADO_GET_ESTORNOS,
    QUERY_LEGADO_GET_ADVICES,
    QUERY_PRESENTE_GET_PICO_TPS_GERAL,
    QUERY_PRESENTE_GET_PICO_TPS_MODERNIZADO,
    QUERY_PRESENTE_GET_PICO_TPS_LEGADO,
    QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD,
    QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_MODERNIZADO,
    QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_LEGADO,
    QUERY_PRESENTE_GET_PICO_TPS_VISA,
    QUERY_PRESENTE_GET_PICO_TPS_VISA_MODERNIZADO,
    QUERY_PRESENTE_GET_PICO_TPS_VISA_LEGADO,
    QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_PRESENTE,
    QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_DIGITAIS,
    QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_LEGADO,
    QUERY_PRESENTE_TOP_10_ERROS
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AthenaReportDataProvider:
    def __init__(self, database: str, output_location: str, athena_client=None):
        self.database = database
        self.output_location = output_location
        self.athena = athena_client or boto3.client('athena')

    def execute(self, date: str) -> dict:
        logger.info("Iniciando obtenção dos dados…")

        dados_presente = self.__get_data(date, QUERY_PRESENTE_BY_STATUS, QUERY_PRESENTE_GET_ESTORNOS, QUERY_PRESENTE_GET_ADVICES)
        dados_digitais = self.__get_data(date, QUERY_DIGITAIS_BY_STATUS, QUERY_DIGITAIS_GET_ESTORNOS, QUERY_DIGITAIS_GET_ADVICES)
        dados_legado = self.__get_data(date, QUERY_LEGADO_BY_STATUS, QUERY_LEGADO_GET_ESTORNOS, QUERY_LEGADO_GET_ADVICES)
        top_10_erros = self.__get_top_10_erros(date)
        dados_tps = self.__get_pico_tps(date)

        data_table = [
            {
                "name": "GERAL",
                "qtd": dados_presente["qtd_aut"] + dados_digitais["qtd_aut"] + dados_legado["qtd_aut"],
                "valor": dados_presente["valor_aut"] + dados_digitais["valor_aut"] + dados_legado["valor_aut"],
                "qtd_neg": dados_presente["qtd_neg_aut"] + dados_digitais["qtd_neg_aut"] + dados_legado["qtd_neg_aut"],
                "valor_neg": dados_presente["valor_neg_aut"] + dados_digitais["valor_neg_aut"] + dados_legado["valor_neg_aut"],
                "qtd_apr": dados_presente["qtd_apr_aut"] + dados_digitais["qtd_apr_aut"] + dados_legado["qtd_apr_aut"],
                "valor_apr": dados_presente["valor_apr_aut"] + dados_digitais["valor_apr_aut"] + dados_legado["valor_apr_aut"],
                "qtd_est": dados_presente["qtd_est_aut"] + dados_digitais["qtd_est_aut"] + dados_legado["qtd_est_aut"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_geral"]
            },
            {
                "name": "MODERNIZADO",
                "qtd": dados_presente["qtd_aut"] + dados_digitais["qtd_aut"],
                "valor": dados_presente["valor_aut"] + dados_digitais["valor_aut"],
                "qtd_neg": dados_presente["qtd_neg_aut"] + dados_digitais["qtd_neg_aut"],
                "valor_neg": dados_presente["valor_neg_aut"] + dados_digitais["valor_neg_aut"],
                "qtd_apr": dados_presente["qtd_apr_aut"] + dados_digitais["qtd_apr_aut"],
                "valor_apr": dados_presente["valor_apr_aut"] + dados_digitais["valor_apr_aut"],
                "qtd_est": dados_presente["qtd_est_aut"] + dados_digitais["qtd_est_aut"],
                "qtd_adv": dados_presente["qtd_adv_aut"] + dados_digitais["qtd_adv_aut"],
                "tps": dados_tps["tps_modernizado"]
            },
            {
                "name": "LEGADO",
                "qtd": dados_legado["qtd_aut"],
                "valor": dados_legado["valor_aut"],
                "qtd_neg": dados_legado["qtd_neg_aut"],
                "valor_neg": dados_legado["valor_neg_aut"],
                "qtd_apr": dados_legado["qtd_apr_aut"],
                "valor_apr": dados_legado["valor_apr_aut"],
                "qtd_est": dados_legado["qtd_est_aut"],
                "qtd_adv": dados_legado["qtd_adv_aut"],
                "tps": dados_tps["tps_legado"]
            },
            {
                "name": "MASTERCARD",
                "qtd": dados_presente["qtd_master"] + dados_digitais["qtd_master"] + dados_legado["qtd_master"],
                "valor": dados_presente["valor_master"] + dados_digitais["valor_master"] + dados_legado["valor_master"],
                "qtd_neg": dados_presente["qtd_neg_master"] + dados_digitais["qtd_neg_master"] + dados_legado["qtd_neg_master"],
                "valor_neg": dados_presente["valor_neg_master"] + dados_digitais["valor_neg_master"] + dados_legado["valor_neg_master"],
                "qtd_apr": dados_presente["qtd_apr_master"] + dados_digitais["qtd_apr_master"] + dados_legado["qtd_apr_master"],
                "valor_apr": dados_presente["valor_apr_master"] + dados_digitais["valor_apr_master"] + dados_legado["valor_apr_master"],
                "qtd_est": dados_presente["qtd_est_master"] + dados_digitais["qtd_est_master"] + dados_legado["qtd_est_master"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_master"]
            },
            {
                "name": "MASTERCARD_MODERNIZADO",
                "qtd": dados_presente["qtd_master"] + dados_digitais["qtd_master"],
                "valor": dados_presente["valor_master"] + dados_digitais["valor_master"],
                "qtd_neg": dados_presente["qtd_neg_master"] + dados_digitais["qtd_neg_master"],
                "valor_neg": dados_presente["valor_neg_master"] + dados_digitais["valor_neg_master"],
                "qtd_apr": dados_presente["qtd_apr_master"] + dados_digitais["qtd_apr_master"],
                "valor_apr": dados_presente["valor_apr_master"] + dados_digitais["valor_apr_master"],
                "qtd_est": dados_presente["qtd_est_master"] + dados_digitais["qtd_est_master"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_master_modern"]
            },
            {
                "name": "MASTERCARD_LEGADO",
                "qtd": dados_legado["qtd_master"],
                "valor": dados_legado["valor_master"],
                "qtd_neg": dados_legado["qtd_neg_master"],
                "valor_neg": dados_legado["valor_neg_master"],
                "qtd_apr": dados_legado["qtd_apr_master"],
                "valor_apr": dados_legado["valor_apr_master"],
                "qtd_est": dados_legado["qtd_est_master"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_master_legado"]
            },
            {
                "name": "VISA",
                "qtd": dados_presente["qtd_visa"] + dados_digitais["qtd_visa"] + dados_legado["qtd_visa"],
                "valor": dados_presente["valor_visa"] + dados_digitais["valor_visa"] + dados_legado["valor_visa"],
                "qtd_neg": dados_presente["qtd_neg_visa"] + dados_digitais["qtd_neg_visa"] + dados_legado["qtd_neg_visa"],
                "valor_neg": dados_presente["valor_neg_visa"] + dados_digitais["valor_neg_visa"] + dados_legado["valor_neg_visa"],
                "qtd_apr": dados_presente["qtd_apr_visa"] + dados_digitais["qtd_apr_visa"] + dados_legado["qtd_apr_visa"],
                "valor_apr": dados_presente["valor_apr_visa"] + dados_digitais["valor_apr_visa"] + dados_legado["valor_apr_visa"],
                "qtd_est": dados_presente["qtd_est_visa"] + dados_digitais["qtd_est_visa"] + dados_legado["qtd_est_visa"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_visa"]
            },
            {
                "name": "VISA_MODERNIZADO",
                "qtd": dados_presente["qtd_visa"] + dados_digitais["qtd_visa"],
                "valor": dados_presente["valor_visa"] + dados_digitais["valor_visa"],
                "qtd_neg": dados_presente["qtd_neg_visa"] + dados_digitais["qtd_neg_visa"],
                "valor_neg": dados_presente["valor_neg_visa"] + dados_digitais["valor_neg_visa"],
                "qtd_apr": dados_presente["qtd_apr_visa"] + dados_digitais["qtd_apr_visa"],
                "valor_apr": dados_presente["valor_apr_visa"] + dados_digitais["valor_apr_visa"],
                "qtd_est": dados_presente["qtd_est_visa"] + dados_digitais["qtd_est_visa"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_visa_modern"]
            },
            {
                "name": "VISA_LEGADO",
                "qtd": dados_legado["qtd_visa"],
                "valor": dados_legado["valor_visa"],
                "qtd_neg": dados_legado["qtd_neg_visa"],
                "valor_neg": dados_legado["valor_neg_visa"],
                "qtd_apr": dados_legado["qtd_apr_visa"],
                "valor_apr": dados_legado["valor_apr_visa"],
                "qtd_est": dados_legado["qtd_est_visa"],
                "qtd_adv": '-',
                "tps": dados_tps["tps_visa_legado"]
            },
            {
                "name": "AUTORIZADOR_PRESENTE",
                "qtd": dados_presente["qtd_aut"],
                "valor": dados_presente["valor_aut"],
                "qtd_neg": dados_presente["qtd_neg_aut"],
                "valor_neg": dados_presente["valor_neg_aut"],
                "qtd_apr": dados_presente["qtd_apr_aut"],
                "valor_apr": dados_presente["valor_apr_aut"],
                "qtd_est": dados_presente["qtd_est_aut"],
                "qtd_adv": dados_presente["qtd_adv_aut"],
                "tps": dados_tps["tps_aut_presente"]
            },
            {
                "name": "AUTORIZADOR_DIGITAIS",
                "qtd": dados_digitais["qtd_aut"],
                "valor": dados_digitais["valor_aut"],
                "qtd_neg": dados_digitais["qtd_neg_aut"],
                "valor_neg": dados_digitais["valor_neg_aut"],
                "qtd_apr": dados_digitais["qtd_apr_aut"],
                "valor_apr": dados_digitais["valor_apr_aut"],
                "qtd_est": dados_digitais["qtd_est_aut"],
                "qtd_adv": dados_digitais["qtd_adv_aut"],
                "tps": dados_tps["tps_aut_digitais"]
            },
            {
                "name": "AUTORIZADOR_LEGADO",
                "qtd": dados_legado["qtd_aut"],
                "valor": dados_legado["valor_aut"],
                "qtd_neg": dados_legado["qtd_neg_aut"],
                "valor_neg": dados_legado["valor_neg_aut"],
                "qtd_apr": dados_legado["qtd_apr_aut"],
                "valor_apr": dados_legado["valor_apr_aut"],
                "qtd_est": dados_legado["qtd_est_aut"],
                "qtd_adv": dados_legado["qtd_adv_aut"],
                "tps": dados_tps["tps_aut_legado"]
            }
        ]

        data_transaction_report = {
            "date_base": date,
            "top_10_erro": top_10_erros,
            "rows": data_table
        }

        return data_transaction_report

    def __get_data(self, date: str, query_by_status: str, query_get_estornos: str, query_get_advices: str) -> dict:
        result_query_by_status = self.__execute_query(date, query_by_status)
        data_by_status = self.__get_fields(result_query_by_status)

        result_query_estornos = self.__execute_query(date, query_get_estornos)
        data_estornos = self.__get_fields_estorno(result_query_estornos)

        result_query_advices = self.__execute_query(date, query_get_advices)
        data_advice = self.__get_fields_advices(result_query_advices)

        return {
            "qtd_visa": data_by_status["qtd_apr_visa"] + data_by_status["qtd_neg_visa"],
            "valor_visa": data_by_status["valor_apr_visa"] + data_by_status["valor_neg_visa"],
            "qtd_apr_visa": data_by_status["qtd_apr_visa"],
            "valor_apr_visa": data_by_status["valor_apr_visa"],
            "qtd_neg_visa": data_by_status["qtd_neg_visa"],
            "valor_neg_visa": data_by_status["valor_neg_visa"],
            "qtd_master": data_by_status["qtd_apr_master"] + data_by_status["qtd_neg_master"],
            "valor_master": data_by_status["valor_apr_master"] + data_by_status["valor_neg_master"],
            "qtd_apr_master": data_by_status["qtd_apr_master"],
            "valor_apr_master": data_by_status["valor_apr_master"],
            "qtd_neg_master": data_by_status["qtd_neg_master"],
            "valor_neg_master": data_by_status["valor_neg_master"],
            "qtd_est_visa": data_estornos["qtd_visa"],
            "qtd_est_master": data_estornos["qtd_master"],
            "qtd_apr_aut": data_by_status["qtd_apr_visa"] + data_by_status["qtd_apr_master"],
            "qtd_neg_aut": data_by_status["qtd_neg_visa"] + data_by_status["qtd_neg_master"],
            "qtd_est_aut": data_estornos["qtd_visa"] + data_estornos["qtd_master"],
            "qtd_adv_aut": data_advice["qtd"],
            "valor_apr_aut": data_by_status["valor_apr_visa"] + data_by_status["valor_apr_master"],
            "valor_neg_aut": data_by_status["valor_neg_visa"] + data_by_status["valor_neg_master"],
            "valor_aut": data_by_status["valor_apr_visa"] + data_by_status["valor_apr_master"] + data_by_status["valor_neg_visa"] + data_by_status["valor_neg_master"],
            "qtd_aut": data_by_status["qtd_apr_visa"] + data_by_status["qtd_apr_master"] + data_by_status["qtd_neg_visa"] + data_by_status["qtd_neg_master"],
        }

    def __execute_query(self, date: str, query: str):
        logger.info(f"Executando query no Athena: {query.format(dia_mes_ano=date)}")

        try:
            response = self.athena.start_query_execution(
                QueryString=query.format(dia_mes_ano=date),
                QueryExecutionContext={"Database": self.database},
                ResultConfiguration={"OutputLocation": self.output_location})

            execution_id = response['QueryExecutionId']

            while True:
                result = self.athena.get_query_execution(QueryExecutionId=execution_id)
                status = result['QueryExecution']['Status']['State']
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)

            result_response = self.athena.get_query_results(QueryExecutionId=execution_id)

            rows = result_response["ResultSet"]["Rows"]
            num_data_rows = len(rows) - 1
            logger.info(f"Fim da execução da query. Quantidade de linhas retornadas: {num_data_rows}")

            return result_response

        except Exception as e:
            logger.error(f"Erro ao executar query: {query}")
            logger.exception("Erro inesperado ao iniciar query Athena")
            self.stop()

    #            raise

    def stop(self):
        logger.info("stop, devido erro")

    @staticmethod
    def __parse_athena(raw_result: dict) -> list[dict]:
        # lista de colunas
        cols = [col["Name"] for col in raw_result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]

        # ignora a primeira linha (cabeçalho)
        data_rows = raw_result["ResultSet"]["Rows"][1:]

        parsed = []
        for row in data_rows:
            # extrai cada valor como string
            values = [field.get("VarCharValue") for field in row["Data"]]
            row_dict = {}
            for name, val in zip(cols, values):
                # decide se converte ou não
                if name in ("quantidade", "valor_total", "pico_tps"):
                    # se vier None ou vazio, trata como zero
                    row_dict[name] = int(val) if val not in (None, "") else 0
                else:
                    row_dict[name] = val
            parsed.append(row_dict)

        return parsed

    def __get_fields(self, raw: dict):
        data_parsed = self.__parse_athena(raw)

        data_map: dict[str, dict[str, dict]] = defaultdict(dict)
        for row in data_parsed:
            data_map[row["bandeira"]][row["status"]] = row

        return {
            "qtd_apr_visa": data_map.get("VISA", {}).get("aprovado", {}).get("quantidade", 0),
            "valor_apr_visa": data_map.get("VISA", {}).get("aprovado", {}).get("valor_total", 0),
            "qtd_neg_visa": data_map.get("VISA", {}).get("negado", {}).get("quantidade", 0),
            "valor_neg_visa": data_map.get("VISA", {}).get("negado", {}).get("valor_total", 0),
            "qtd_apr_master": data_map.get("MASTERCARD", {}).get("aprovado", {}).get("quantidade", 0),
            "valor_apr_master": data_map.get("MASTERCARD", {}).get("aprovado", {}).get("valor_total", 0),
            "qtd_neg_master": data_map.get("MASTERCARD", {}).get("negado", {}).get("quantidade", 0),
            "valor_neg_master": data_map.get("MASTERCARD", {}).get("aprovado", {}).get("valor_total", 0)
        }

    def __get_fields_estorno(self, raw):
        data_parsed = self.__parse_athena(raw)

        data_map: dict[str, int] = defaultdict(int)
        for row in data_parsed:
            bandeira = row["bandeira"]
            data_map[bandeira] += row.get("quantidade", 0)

        qtd_visa = data_map.get("VISA", 0)
        qtd_master = data_map.get("MASTERCARD", 0)

        return {
            "qtd_visa": qtd_visa,
            "qtd_master": qtd_master}

    def __get_fields_advices(self, raw):
        data_parsed = self.__parse_athena(raw)
        qtd = data_parsed[0]["quantidade"]
        return {"qtd": qtd}

    def __get_pico_tps(self, date: str):
        tps_geral = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_GERAL)
        tps_modernizado = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_MODERNIZADO)
        tps_legado = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_LEGADO)
        tps_master = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD)
        tps_master_modern = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_MODERNIZADO)
        tps_master_legado = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_MASTERCARD_LEGADO)
        tps_visa = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_VISA)
        tps_visa_modern = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_VISA_MODERNIZADO)
        tps_visa_legado = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_VISA_LEGADO)
        tps_aut_presente = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_PRESENTE)
        tps_aut_digitais = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_DIGITAIS)
        tps_aut_legado = self.__get_data_pico_tps(date, QUERY_PRESENTE_GET_PICO_TPS_AUTORIZADOR_LEGADO)

        return {
            "tps_geral": tps_geral,
            "tps_modernizado": tps_modernizado,
            "tps_legado": tps_legado,
            "tps_master": tps_master,
            "tps_master_modern": tps_master_modern,
            "tps_master_legado": tps_master_legado,
            "tps_visa": tps_visa,
            "tps_visa_modern": tps_visa_modern,
            "tps_visa_legado": tps_visa_legado,
            "tps_aut_presente": tps_aut_presente,
            "tps_aut_digitais": tps_aut_digitais,
            "tps_aut_legado": tps_aut_legado,
        }

    def __get_data_pico_tps(self, date: str, query: str):
        result_query_by_status = self.__execute_query(date, query)
        data_parsed = self.__parse_athena(result_query_by_status)

        pico_tps = data_parsed[0].get("pico_tps", 0) if data_parsed else 0
        data_hora = data_parsed[0].get("data_hora", 0) if data_parsed else 0

        try:
            dt = datetime.fromisoformat(data_hora)
            hora = dt.strftime("%H:%M:%S")
        except ValueError:
            hora = "hh:mm:ss"

        return f"{pico_tps} - {hora}"

    def __get_top_10_erros(self, date: str):
        result_query = self.__execute_query(date, QUERY_PRESENTE_TOP_10_ERROS)
        data_parsed = self.__parse_athena(result_query)

        # agrupa por bandeira
        grupos = defaultdict(list)
        for e in data_parsed:
            grupos[e['bandeira']].append(e)

        # para cada bandeira, ordena por quantidade desc e mapeia para o formato desejado
        return {
            bandeira: [
                {
                    "cod_ret":  item["codigo_retorno"],
                    "qtd":      item["quantidade"],
                    "desc":     item["desc_erro"]
                }
                for item in sorted(lista, key=lambda x: x["quantidade"], reverse=True)[:10]
            ]
            for bandeira, lista in grupos.items()
        }
        # return {
        #     "MASTERCARD": [
        #         {"cod_ret": "51", "qtd": 123456789, "desc": "Cartão expirado"},
        #         {"cod_ret": "55", "qtd": 12345678, "desc": "Saldo insuficiente"},
        #         {"cod_ret": "75", "qtd": 1234567, "desc": "Limite excedido"},
        #         {"cod_ret": "14", "qtd": 123456, "desc": "Código de segurança inválido"},
        #         {"cod_ret": "57", "qtd": 12345, "desc": "Cartão bloqueado"},
        #         {"cod_ret": "85", "qtd": 1234, "desc": "Transação não autorizada"},
        #         {"cod_ret": "12", "qtd": 123, "desc": "Operação não suportada"},
        #         {"cod_ret": "96", "qtd": 12, "desc": "Dados do titular inválidos"},
        #         {"cod_ret": "91", "qtd": 0, "desc": "Falha na comunicação com a operadora"},
        #         {"cod_ret": "58", "qtd": 0, "desc": "Erro genérico da rede"}
        #     ],
        #     "VISA": [
        #         {"cod_ret": "51", "qtd": 200, "desc": "Cartão expirado"},
        #         {"cod_ret": "63", "qtd": 180, "desc": "Saldo insuficiente"},
        #         {"cod_ret": "06", "qtd": 130, "desc": "Limite excedido"},
        #         {"cod_ret": "14", "qtd": 95, "desc": "Código de segurança inválido"},
        #         {"cod_ret": "5C", "qtd": 90, "desc": "Cartão bloqueado"},
        #         {"cod_ret": "91", "qtd": 70, "desc": "Transação não autorizada"},
        #         {"cod_ret": "57", "qtd": 60, "desc": "Operação não suportada"},
        #         {"cod_ret": "58", "qtd": 55, "desc": "Dados do titular inválidos"},
        #         {"cod_ret": "57", "qtd": 40, "desc": "Falha na comunicação com a operadora"},
        #         {"cod_ret": "30", "qtd": 35, "desc": "Erro genérico da rede"}
        #     ]
        # }
