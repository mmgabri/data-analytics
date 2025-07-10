import json
import boto3

sns = boto3.client("sns")
TOPIC_ARN      = "arn:aws:sns:us-east-1:140023369634:notification-agilfacil"

def lambda_handler(event, context):
    # Versão texto puro (fallback)
    plain_text = "Olá,\n\nEste é um teste de e-mail HTML via SNS.\n\nAbra no cliente para ver o HTML."

    # Versão HTML simples com fonte monoespaçada
    html_body = """<html>
  <body>
    <h3>Relatório Transacional</h3>
    <pre style="font-family: monospace; line-height:1.2; border:1px solid #ccc; padding:10px;">
╔══════════════╦═══════╦═════════╗
║ Linha        ║ QTD   ║ Valor   ║
╠══════════════╬═══════╬═════════╣
║ GERAL        ║  1234 ║ R$1.000,12 ║
║ MODERNIZADO  ║   567 ║ R$5.000,34 ║
╚══════════════╩═══════╩═════════╝
    </pre>
  </body>
</html>"""

    message = {
        "default": plain_text,
        "email": html_body
    }

    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject="Teste HTML via SNS",
        Message=json.dumps(message),
        MessageStructure="json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "E-mail enviado via SNS"})
    }


╔