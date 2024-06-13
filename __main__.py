import os
import wget
import pandas as pd

# Defining aux variables

FILE_NAME = "IGR.csv"

YEAR = "2023"

URL = f"""https://dadosabertos.ans.gov.br/FTP/PDA/IGR/IGR_versao_{YEAR}/{FILE_NAME}"""

PARQUET_NAME = "IGR.parquet"

def download_file():

    wget.download(URL, FILE_NAME)

    return True

def read_file():

    df = pd.read_csv(FILE_NAME, sep=';', on_bad_lines='skip')
    return df

if __name__ == "__main__":
    
    download_file()
    print(read_file().head())