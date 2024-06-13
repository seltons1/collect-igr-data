import os
import datetime
import wget

# Defining aux variables

FILE_NAME = "IGR.csv"

URL = f"""https://dadosabertos.ans.gov.br/FTP/PDA/IGR/IGR_versao_2023/{FILE_NAME}"""

PARQUET_NAME = "IGR.parquet"


if __name__ == "__main__":
    pass