import os
import wget
import pandas as pd

# Defining aux variables

FILE_NAME = "IGR.csv"

YEAR = "2023"

URL = f"""https://dadosabertos.ans.gov.br/FTP/PDA/IGR/IGR_versao_{YEAR}/{FILE_NAME}"""

PARQUET_NAME = f"""silver/IGR_{YEAR}.parquet"""

def download_file() -> None:
    """

    Download file from URL previously defined.
    
    """

    wget.download(URL, 'raw/'+FILE_NAME)

def remove_old_file():
    """

    Remove previously .csv file
    
    """

    if os.path.exists('raw/'+FILE_NAME):

        os.remove('raw/'+FILE_NAME)

    return True

def read_file() -> pd.DataFrame:
    """

    Read .csv and return Dataframe
    
    """

    dtype = {"REGISTRO_ANS" : 'int64',
         "RAZAO_SOCIAL" : 'string',
         "COBERTURA": 'string',
         "IGR": 'string',
         "QTD_RECLAMACOES": 'int64',
         "QTD_BENEFICIARIOS": 'int64',
         "PORTE_OPERADORA": 'string',
         "COMPETENCIA": 'int64',
         "COMPETENCIA_BENEFICIARIO": 'int64',
         "DT_ATUALIZACAO": 'string'
       }

    df = pd.read_csv('raw/'+FILE_NAME, sep=';', on_bad_lines='skip', dtype=dtype)

    return df

def write_parquet(df) -> None:
    """

    Create .parquet file
    
    """

    df.to_parquet(PARQUET_NAME ,engine='pyarrow', compression='snappy')


if __name__ == "__main__":
    
    remove_old_file()

    download_file()
    
    df = read_file()
    
    write_parquet(df)
