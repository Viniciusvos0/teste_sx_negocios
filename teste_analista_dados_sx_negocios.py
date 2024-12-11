import pandas as pd

# Iniciando - Bloco 1 - Carregando e Limpando os dados

# Definindo o caminho do arquivo original
file_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DADOS\MICRODADOS_ENEM_2020.csv"

# Definindo as colunas a serem mantidas
columns_to_keep = [
    "NU_INSCRICAO",
    "TP_FAIXA_ETARIA",
    "TP_SEXO",
    "TP_COR_RACA",
    "TP_ST_CONCLUSAO",
    "TP_ESCOLA",
    "TP_PRESENCA_CN",
    "TP_PRESENCA_CH",
    "TP_PRESENCA_LC",
    "TP_PRESENCA_MT",
    "NU_NOTA_CN",
    "NU_NOTA_CH",
    "NU_NOTA_LC",
    "NU_NOTA_MT",
    "NU_NOTA_COMP1",
    "NU_NOTA_COMP2",
    "NU_NOTA_COMP3",
    "NU_NOTA_COMP4",
    "NU_NOTA_COMP5",
    "NU_NOTA_REDACAO",
    "Q006",
    "Q025",
]

# Inicializando o objeto para salvar o arquivo final
output_file_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DADOS\MICRODADOS_ENEM_2020_reduzido.csv"

# Configurando a leitura do arquivo em pedaços
chunk_size = 50000  # Definindo o tamanho do pedaço (pode ser ajustado dependendo da memória disponível)
chunks = pd.read_csv(file_path, delimiter=";", encoding="latin1", chunksize=chunk_size)

# Criando um arquivo vazio para salvar os resultados finais
is_first_chunk = True
for chunk in chunks:
    # Limpando espaços nas colunas
    chunk.columns = chunk.columns.str.strip()

    # Filtrando as colunas necessárias
    chunk_reduced = chunk[columns_to_keep]

    # Salvando o pedaço no arquivo
    if is_first_chunk:
        chunk_reduced.to_csv(
            output_file_path, index=False, mode="w"
        )  # Modo 'w' para criar o arquivo
        is_first_chunk = False
    else:
        chunk_reduced.to_csv(
            output_file_path, index=False, mode="a", header=False
        )  # Modo 'a' para adicionar ao arquivo existente

print(f"Arquivo reduzido salvo em: {output_file_path}")

# Finalizando - Bloco 1 - Carregando e Limpando os dados


# Iniciando - Bloco 2 - Calculando e transformando

# Definindo o caminho do arquivo
file_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DADOS\MICRODADOS_ENEM_2020_reduzido.csv"

# Tentando ler o arquivo com delimitador padrão (vírgula)
df = pd.read_csv(file_path, encoding="latin1")

# Imprimindo os nomes das colunas para verificar se estão corretos
print("Nomes das colunas no arquivo CSV:")
print(df.columns)

# Seguindo com a soma e a média
# Calculando a média das colunas especificadas
df["NU_NOTA_FINAL"] = (
    df[["NU_NOTA_CN", "NU_NOTA_CH", "NU_NOTA_LC", "NU_NOTA_MT", "NU_NOTA_REDACAO"]].sum(
        axis=1
    )
    / 5
)

# Salvando o DataFrame atualizado de volta no arquivo CSV
output_file_path = r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DADOS\MICRODADOS_ENEM_2020_com_media.csv"
df.to_csv(output_file_path, index=False)

# Imprimindo as primeiras 10 linhas do DataFrame
print(df.head(10))

print(f"Arquivo atualizado com a coluna NU_NOTA_FINAL salvo em: {output_file_path}")

# Finalizando - Bloco 2 - Calculando e transformando

# Iniciando - Bloco 3 - Enviando para o Banco de Dados

from sqlalchemy import create_engine
import pandas as pd

# Definindo configurações de conexão
user = "vinicius"
password = "1234root"  # Certificando-se de que esta é a senha correta
host = "172.18.0.1"  # Definindo o IP correto do MySQL no Docker
port = "3306"
database = "prova_enem"  # Certificando-se de que o banco existe

# Criando o motor de conexão
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# Definindo o dicionário com os caminhos dos arquivos e os nomes das tabelas
files_and_tables = {
    "MICRODADOS_ENEM_2020_com_media": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DADOS\MICRODADOS_ENEM_2020_com_media.csv",
    "acesso_a_internet": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\acesso_a_internet.csv",
    "etnia": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\etnia.csv",
    "faixa_etaria": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\faixa_etaria.csv",
    "presenca": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\presenca.csv",
    "redacao": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\redacao.csv",
    "renda": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\renda.csv",
    "sexo": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\sexo.csv",
    "tp_escola": r"C:\Users\vinic\OneDrive\Área de Trabalho\Case SX Negócios\DIM\tp_escola.csv",
}

# Iterando pelos arquivos e carregando-os no banco de dados
for table_name, file_path in files_and_tables.items():
    print(f"Carregando {table_name}...")

    # Lendo o CSV
    df = pd.read_csv(file_path)

    # Enviando para o MySQL
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    print(f"Tabela {table_name} carregada com sucesso.")

# Finalizando - Bloco 3 - Enviando para o Banco de Dados
