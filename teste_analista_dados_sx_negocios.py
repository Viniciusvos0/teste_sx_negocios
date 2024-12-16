import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
import os
import sys
import glob


#Após criar o arquivo yml e o container no docker com docker-compose up -d,
# sigo com o código para fazer o ETL e posteriormente subir o df para o container


# Definindo o Python para o PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Definindo os diretórios do Spark e Hadoop
os.environ['SPARK_HOME'] = r"C:\spark\spark-3.5.3-bin-hadoop3"
os.environ['HADOOP_HOME'] = r"C:\hadoop"

# Configuração do SparkSession
spark = SparkSession.builder \
    .appName("sx_teste_PySpark") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.io.native.lib", "false") \
    .config("spark.sql.warehouse.dir", "C:\\spark\\spark-3.5.3-bin-hadoop3\\spark-warehouse") \
    .getOrCreate()

# Início do bloco (Leitura e carregamento do arquivo CSV com PySpark)
file_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\case_sx_negocios\DADOS\MICRODADOS_ENEM_2020.csv"
df = spark.read.option("delimiter", ";").csv(file_path, header=True, inferSchema=True)
# Fim do bloco (Leitura e carregamento do arquivo CSV)

# Início do bloco (Limpeza das colunas)
colunas_necessarias = [
    "NU_INSCRICAO", "TP_FAIXA_ETARIA", "TP_SEXO", "TP_COR_RACA", "TP_ST_CONCLUSAO", 
    "TP_ESCOLA", "TP_PRESENCA_CN", "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", 
    "NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_COMP1", "NU_NOTA_COMP2", 
    "NU_NOTA_COMP3", "NU_NOTA_COMP4", "NU_NOTA_COMP5", "NU_NOTA_REDACAO", "Q006", "Q025"
]
df_limpo = df.select(colunas_necessarias)
# Fim do bloco (Limpeza das colunas)

# Início do bloco (Cálculo da média das notas)
df_com_media = df_limpo.withColumn(
    "MEDIA_NOTA", 
    (df_limpo["NU_NOTA_CN"] + df_limpo["NU_NOTA_CH"] + df_limpo["NU_NOTA_LC"] + 
     df_limpo["NU_NOTA_MT"] + df_limpo["NU_NOTA_REDACAO"]) / 5
)
# Fim do bloco (Cálculo da média das notas)

# Início do bloco (Conversão para float)
df_com_media = df_com_media.withColumn("NU_NOTA_CN", df_com_media["NU_NOTA_CN"].cast("float"))
df_com_media = df_com_media.withColumn("NU_NOTA_CH", df_com_media["NU_NOTA_CH"].cast("float"))
df_com_media = df_com_media.withColumn("NU_NOTA_LC", df_com_media["NU_NOTA_LC"].cast("float"))
df_com_media = df_com_media.withColumn("NU_NOTA_MT", df_com_media["NU_NOTA_MT"].cast("float"))
df_com_media = df_com_media.withColumn("NU_NOTA_REDACAO", df_com_media["NU_NOTA_REDACAO"].cast("float"))
df_com_media = df_com_media.withColumn("MEDIA_NOTA", df_com_media["MEDIA_NOTA"].cast("float"))
# Fim do bloco (Conversão para float)

# Início do bloco (Salvamento do arquivo CSV limpo)
output_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\case_sx_negocios\DADOS\MICRODADOS_ENEM_2020_T.csv"
# Escrevendo o arquivo com o separador de vírgula
df_com_media.coalesce(1).write.option("header", "true").option("delimiter", ";").csv(output_path)
# Fim do bloco (Salvamento do arquivo CSV limpo)


# Início do bloco (Carregar CSV com pandas e enviar para MySQL)
# Usando glob para pegar todos os arquivos que começam com part-*.csv
csv_files = glob.glob(output_path + '/part-*.csv')

# Lendo todos os arquivos CSV com pandas e concatenando em um único DataFrame
df_pandas = pd.concat([pd.read_csv(f, delimiter=',') for f in csv_files], ignore_index=True)

# Definindo configurações de conexão com o MySQL
user = "vinicius"
password = "1234root"  # Certificando-se de que esta é a senha correta
host = "172.17.0.2"  # Definindo o IP correto do MySQL no Docker
port = "3307"  # Definindo a porta do MySQL no contêiner
database = "prova_enem"  # Certificando-se de que o banco existe

# Montando a URL de conexão
mysql_url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"

# Criando o motor de conexão
engine = create_engine(mysql_url)


# Subindo os dados para o MySQL
df_pandas.to_sql('MICRODADOS_ENEM_2020', con=engine, if_exists='replace', index=False)
# Fim do bloco (Carregar CSV com pandas e enviar para MySQL)
