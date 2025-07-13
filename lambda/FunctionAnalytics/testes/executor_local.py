import sys, os

# 1) Diretorio lambda/ como raiz de import
root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root)

# 2) Agora o Python enxerga FunctionAnalytics/ como pacote de 1º nível
from FunctionAnalytics.data_analytics import lambda_handler

# chame o handler
lambda_handler({}, None)
