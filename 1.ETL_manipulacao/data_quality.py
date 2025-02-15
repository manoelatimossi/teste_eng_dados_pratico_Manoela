from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType
import datetime
#Valida se o user é único. 
def check_user_unico(df):
    dup = df.groupBy("transaction_id").count().filter("count > 1")
    
    if dup.count() == 0:

        print("Pass: Todos os IDs são únicos.")
    else:
        print(f"Fail: Existem {dup.count()} IDs duplicados.")

#Valida se há vendas negativas.
def check_venda_negativa(df):

    negative_sales = df.filter(df["sale_value"] < 0)
    
    if negative_sales.count() == 0:

        print("Pass: Não há vendas negativas.")
    else:
        print(f"Fail: Existem {negative_sales.count()} vendas com valores negativos.")

#Valida se o Timestamp é valido. 
def check_valid_timestamps(df):

    invalid_dates = df.filter(~col("date").rlike(r'^\d{4}-\d{2}-\d{2}$')).count()

    if invalid_dates == 0:
        print("PASS: Todos os timestamps são válidos.")
    else:
        print(f"Fail: Existem {valid_timestamps.count()} registros com timestamps inválidos.")

#Valida se o número do df antes da ingestão é igual ao depois da ingestão.
def check_row_count(df, df_raw):
    ingested_count = df.count()
    df_raw_count = df_raw.count()
    if ingested_count == df_raw_count:
        print("Pass: Ingestão de linhas no db manteve o mesmo número do original!")
    else:
        diff = ((ingested_count - df_raw_count) / ingested_count) * 100
        print(f"Fail: Houve um problema de integridade e o número de linhas está {diff}")

def run_data_quality_checks(df, df_raw):
    checks = {
        "Unique User IDs": check_user_unico(df),
        "Non-Negative Sales": check_venda_negativa(df),
        "Valid Timestamps": check_valid_timestamps(df),
        "Row Count Match": check_row_count(df, df_raw)
    }
    
    