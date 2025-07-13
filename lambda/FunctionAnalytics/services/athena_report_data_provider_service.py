import logging
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict

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

        dados_presente = self.__get_data(QUERY_PRESENTE_BY_STATUS.format(dia_mes_ano=date), QUERY_PRESENTE_GET_ESTORNOS.format(dia_mes_ano=date), QUERY_PRESENTE_GET_ADVICES.format(dia_mes_ano=date))
        dados_digitais = self.__get_data(QUERY_DIGITAIS_BY_STATUS.format(dia_mes_ano=date), QUERY_DIGITAIS_GET_ESTORNOS.format(dia_mes_ano=date), QUERY_DIGITAIS_GET_ADVICES.format(dia_mes_ano=date))
        dados_legado = self.__get_data(QUERY_LEGADO_BY_STATUS.format(dia_mes_ano=date), QUERY_LEGADO_GET_ESTORNOS.format(dia_mes_ano=date), QUERY_LEGADO_GET_ADVICES.format(dia_mes_ano=date))

        return {
            "rows": [
                {
                    "name": "GERAL",
                    "qtd": dados_presente["qtd_aut"] + dados_digitais["qtd_aut"] + dados_legado["qtd_aut"],
                    "valor": dados_presente["valor_aut"] + dados_digitais["valor_aut"] + dados_legado["valor_aut"],
                    "qtd_neg": dados_presente["qtd_neg_aut"] + dados_digitais["qtd_neg_aut"] + dados_legado["qtd_neg_aut"],
                    "valor_neg": dados_presente["valor_neg_aut"] + dados_digitais["valor_neg_aut"] + dados_legado["valor_neg_aut"],
                    "qtd_apr": dados_presente["qtd_apr_aut"] + dados_digitais["qtd_apr_aut"] + dados_legado["qtd_apr_aut"],
                    "valor_apr": dados_presente["valor_apr_aut"] + dados_digitais["valor_apr_aut"] + dados_legado["valor_apr_aut"],
                    "qtd_est": dados_presente["qtd_est_aut"] + dados_digitais["qtd_est_aut"] + dados_legado["qtd_est_aut"],
                    "qtd_adv": dados_presente["qtd_adv_aut"] + dados_digitais["qtd_adv_aut"] + dados_legado["qtd_adv_aut"],
                    "tps": 0
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
                    "tps": 0
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
                    "tps": 0
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
                    "qtd_adv": dados_presente["qtd_adv_master"] + dados_digitais["qtd_adv_master"] + dados_legado["qtd_adv_master"],
                    "tps": 0
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
                    "qtd_adv": dados_presente["qtd_adv_master"] + dados_digitais["qtd_adv_master"],
                    "tps": 0
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
                    "qtd_adv": dados_legado["qtd_adv_master"],
                    "tps": 0
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
                    "qtd_adv": dados_presente["qtd_adv_visa"] + dados_digitais["qtd_adv_visa"] + dados_legado["qtd_adv_visa"],
                    "tps": 0
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
                    "qtd_adv": dados_presente["qtd_adv_visa"] + dados_digitais["qtd_adv_visa"],
                    "tps": 0
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
                    "qtd_adv": dados_legado["qtd_adv_visa"],
                    "tps": 0
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
                    "tps": 0
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
                    "tps": 0
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
                    "tps": 0
                }
            ]}

    def __get_data(self, query_by_status: str, query_get_estornos: str, query_get_advices: str) -> dict:
        result_query_by_status = self.__execute_query(query_by_status)
        result_query_estornos = self.__execute_query(query_get_estornos)
        result_query_advices = self.__execute_query(query_get_advices)

        data_by_status = self.__get_fields("status", "aprovado", "negado", result_query_by_status)
        data_estornos = self.__get_fields("tipo", "estorno", None, result_query_estornos)
        data_advice = self.__get_fields("tipo", "advice", None, result_query_advices)

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
            "qtd_est_visa": data_estornos["qtd_est_visa"],
            "valor_est_visa": data_estornos["valor_est_visa"],
            "qtd_est_master": data_estornos["qtd_est_master"],
            "valor_est_master": data_estornos["valor_est_master"],
            "qtd_adv_visa": data_advice["qtd_adv_visa"],
            "valor_adv_visa": data_advice["valor_adv_visa"],
            "qtd_adv_master": data_advice["qtd_adv_master"],
            "valor_adv_master": data_advice["valor_adv_master"],
            "qtd_apr_aut": data_by_status["qtd_apr_visa"] + data_by_status["qtd_apr_master"],
            "qtd_neg_aut": data_by_status["qtd_neg_visa"] + data_by_status["qtd_neg_master"],
            "qtd_est_aut": data_estornos["qtd_est_visa"] + data_estornos["qtd_est_master"],
            "qtd_adv_aut": data_advice["qtd_adv_visa"] + data_advice["qtd_adv_master"],
            "valor_apr_aut": data_by_status["valor_apr_visa"] + data_by_status["valor_apr_master"],
            "valor_neg_aut": data_by_status["valor_neg_visa"] + data_by_status["valor_neg_master"],
            "valor_aut": data_by_status["valor_apr_visa"] + data_by_status["valor_apr_master"] + data_by_status["valor_neg_visa"] + data_by_status["valor_neg_master"],
            "qtd_aut": data_by_status["qtd_apr_visa"] + data_by_status["qtd_apr_master"] + data_by_status["qtd_neg_visa"] + data_by_status["qtd_neg_master"],
        }

    def __execute_query(self, query:str) -> Dict[str, Any]:
        logger.info(f"Executando query no Athena: {query}")

        try:
            response = self.athena.start_query_execution(
                QueryString=query,
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

    def __get_fields(self, key: str, value1: str, value2: str, raw: dict):
        data_parsed = self.__parse_athena(raw)

        data_map: dict[str, dict[str, dict]] = defaultdict(dict)
        for row in data_parsed:
            data_map[row["bandeira"]][row[key]] = row

        if value2 is None:
            qtd_value1_visa = data_map.get("VISA", {}).get(value1, {}).get("quantidade", 0)
            valor_value1_visa = data_map.get("VISA", {}).get(value1, {}).get("valor_total", 0)
            qtd_value1_master = data_map.get("MASTERCARD", {}).get(value1, {}).get("quantidade", 0)
            valor_value1_master = data_map.get("MASTERCARD", {}).get(value1, {}).get("valor_total", 0)
            if value1 == 'estorno':
                return {
                    "qtd_est_visa": qtd_value1_visa,
                    "valor_est_visa": valor_value1_visa,
                    "qtd_est_master": qtd_value1_master,
                    "valor_est_master": valor_value1_master}
            else:
                return {
                    "qtd_adv_visa": qtd_value1_visa,
                    "valor_adv_visa": valor_value1_visa,
                    "qtd_adv_master": qtd_value1_master,
                    "valor_adv_master": valor_value1_master}

        return {
            "qtd_apr_visa": data_map.get("VISA", {}).get(value1, {}).get("quantidade", 0),
            "valor_apr_visa": data_map.get("VISA", {}).get(value1, {}).get("valor_total", 0),
            "qtd_neg_visa": data_map.get("VISA", {}).get(value2, {}).get("quantidade", 0),
            "valor_neg_visa": data_map.get("VISA", {}).get(value2, {}).get("valor_total", 0),
            "qtd_apr_master": data_map.get("MASTERCARD", {}).get(value1, {}).get("quantidade", 0),
            "valor_apr_master": data_map.get("MASTERCARD", {}).get(value1, {}).get("valor_total", 0),
            "qtd_neg_master": data_map.get("MASTERCARD", {}).get(value2, {}).get("quantidade", 0),
            "valor_neg_master": data_map.get("MASTERCARD", {}).get(value2, {}).get("valor_total", 0)
        }

    def __get_pico_tps(self, raw: dict):
        data_parsed = self.__parse_athena(raw)

        pico_tps = data_parsed[0].get("pico_tps", 0) if data_parsed else 0
        data_hora = data_parsed[0].get("data_hora", 0) if data_parsed else 0

        try:
            dt = datetime.fromisoformat(data_hora)
            hora = dt.strftime("%H:%M:%S")
        except ValueError:
            hora = "hh:mm:ss"

        return f"{pico_tps} - {hora}"