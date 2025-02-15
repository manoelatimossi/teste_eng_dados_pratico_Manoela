import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from main_etl import limpeza_dados  
class TestLimpezaDados(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        
        cls.spark = SparkSession.builder \
            .appName("TesteUnitarioLimpezaDados") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        
        cls.spark.stop()

    def test_happy_path(self):
        # Cria um Df de teste com duplicatas e valores ausentes 
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("sale_value", DoubleType(), True)
        ])
        data = [
            ("2001", "1005", 100.0),
            ("888", None, 200.0),
            ("3555", "2004", None),
            ("2001", "1005", 100.0)  # Duplicata para testar a função
        ]
        df = self.spark.createDataFrame(data, schema)

        # Aplica a função de limpeza
        df_limpo = limpeza_dados(df)
        
        # Verifica se as duplicatas foram removidas
        self.assertEqual(df_limpo.count(), 3)

        # Verifica se os valores ausentes foram tratados
        result = df_limpo.collect()
        self.assertEqual(result[0]["transaction_id"], "2001")
        self.assertEqual(result[1]["product_id"], "Vazio")  # Valor ausente substituído
        self.assertEqual(result[2]["sale_value"], 0.0)  # Valor ausente substituído

    #Para os casos de borda, temos o dataframe vazio ou sem valores repetidos. 
    def test_edge_case_empty_dataframe(self):

        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("sale_value", DoubleType(), True)
        ])
        df = self.spark.createDataFrame([], schema)

        df_limpo = limpeza_dados(df)
        
        # Verifica se o DataFrame permanece vazio
        self.assertEqual(df_limpo.count(), 0)
    
# Cria um DataFrame sem duplicatas ou valores ausentes
    def test_edge_case_no_missing_or_duplicates(self):

        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("sale_value", DoubleType(), True)
        ])
        data = [
            ("1005", "2005", 100.0),
            ("1006", "2007", 200.0)
        ]
        df = self.spark.createDataFrame(data, schema)

        df_limpo = limpeza_dados(df)

        self.assertEqual(df_limpo.collect(), df.collect())

 # Testa o comportamento da função com um DataFrame nulo
    def test_error_case_null_dataframe(self):
       
        with self.assertRaises(AttributeError):
            limpeza_dados(None)

if __name__ == '__main__':
    unittest.main_etl()