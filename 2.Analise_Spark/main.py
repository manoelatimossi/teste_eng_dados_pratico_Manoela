from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg,weekofyear

def top_10_pags(df):
  resultado  = df.groupBy("page_url").agg(count("*").alias("visit_count")) \
                     .orderBy(col("visit_count").desc()) \
                     .limit(10)
  return resultado

def media_dur_sessao(df):
    media_sessao = df.agg(avg("session_duration").alias("avg_session_duration"))

    return media_sessao

def retorno_usuario (df):

    #Primeiro, precisamos adicionar uma coluna para a semana do ano
    df_semana = df.withColumn("week", weekofyear(col("date")))
    
    # Depois, vamos contar o número de visitas por usuário por semana
    visita_semanal = df_semana.groupBy("user_id", "week").agg(count("*").alias("visit_count"))
    
    # Filtrar usuários que visitaram mais de uma vez por semana
    usuarios = visita_semanal.filter(col("visit_count") > 1) \
                                        .select("user_id").distinct()
    
    # Contar o número de usuários que retornam
    num_retorno_usuario = usuarios.count()
    return num_retorno_usuario


if __name__ == "__main__":
    spark = SparkSession.builder.appName("tech_itau_analytics").getOrCreate()

    # Carrega o df
    df = spark.read.csv("website_logs.csv", header=True, inferSchema=True)

    print('top 10 páginas visitadas:')
    top10 = top_10_pags(df)
    top10.show()

    print('Média de duração de sessão:')
    media_sessao = media_dur_sessao(df)
    media_sessao.show()

    print('Número de usuários que retornam:')
    num_retorno_usuario = retorno_usuario(df)
    print(num_retorno_usuario)
