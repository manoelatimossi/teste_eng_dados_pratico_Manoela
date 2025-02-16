from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
#from data_quality import run_data_quality_checks

def limpeza_dados(df):
    #limpa as linhas duplicadas
    df = df.dropDuplicates()

    # Tratamento de valores ausentes:
    # Preencher valores ausentes em colunas numéricas com 0 e as sting com vazio
    numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type in ('int', 'double', 'float')]
    for col_name in numeric_columns:
        df = df.fillna(0, subset=[col_name])
    string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
    for col_name in string_columns:
        df = df.fillna("Vazio", subset=[col_name])
    return df

def modifica_moeda (df):
   df = df.withColumn("sale_value", col("sale_value") * 0.75).withColumn("currency", when(col("currency") == "FICT", "USD").otherwise(col("currency")))

   return df
if __name__ == "__main__":
    spark = SparkSession.builder.appName("tech_itau").config("spark.jars","/home/manoe/itau/.env/1.ETL_manipulacao/mysql-connector-j-9.2.0.jar").getOrCreate()
    jdbc_url = "jdbc:mysql://xxx.xxx.x.xxx:3306/itau?useSSL=false&allowPublicKeyRetrieval=true" #aqui tem esse xxx pois eu utilizei o meu ip para conectar pois estava desenvolvendo no wsl. 
    mysql_properties = {
        "user": "Manoela",
        "password": "1234",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # Carrega o df
    df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
    #Primeiro, limpamos os dados
    df_limpo = limpeza_dados(df)
    # Agora, fazemos a conversão
    df_novo = modifica_moeda(df_limpo)
    #Inserindo no mysql
    df_novo.write.jdbc(url=jdbc_url, table="sales_data", mode="append", properties=mysql_properties)

    #Lendo a tabela que acabamos de inserir para fazer o data quality e comparar o número de linhas com o da ingestão: 
    df_mysql = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "sales_data") \
    .option("user", mysql_properties["user"]) \
    .option("password", mysql_properties["password"]) \
    .option("driver", mysql_properties["driver"]) \
    .load()
    #Fazendo o Data Quality:
    run_data_quality_checks(df_novo,df_mysql)

    