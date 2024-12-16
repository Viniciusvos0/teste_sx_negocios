No teste foram utilizadas as seguintes ferramentas:
Python:
PySpark
sqlalchemy
os
sys
Pandas

No python foram realizadas as primeiras etapas do ETL. Foi feita a ingestão da database, em seguida o df foi limpo e foram feitas as etapas de limpeza, calculos e transformações.
Após essas etapas foi feito o salvamento do arquivo limpo e transformado em um novo arquivo .csv
Todas essas etapas foram feitas utilizando PySpark e bibliotecas nativas(os, sys).

Após realizar essas etapas foi necessário utilizar a biblioteca pandas e a engine do sqlalchemy pois não consegui conectar com o banco de dados pelo pyspark.

No fim não onsegui conectar com o banco de dados mas criei um banco de dados com os arquivos necessários, deixarei o arquivo do banco de dados disponíveis para análise.

No Power Bi tive um problema na conexão com o banco de dados então obtive os arquivos diretamente.

Entreguei todas as respostas no arquivo power bi desde as perguntas iniciais até os bullets de insights.

o link do Power Bi online é o:
https://app.powerbi.com/view?r=eyJrIjoiZjg4MGVmMDUtYmZiZS00MDZiLWE0ZTAtNDQzMWQ4ODQ2YThmIiwidCI6IjBjYTZlNGEyLTU2Y2ItNGMzZi05MGNhLWZkM2UwYjZhYzFmNSJ9

O arquivo do Power bi está disponível para download no link:

https://github.com/Viniciusvos0/teste_sx_negocios/releases/download/release-2024-12/Teste_SX_Negocios.pbix

E o banco de dados está disponível para download no link:

https://github.com/Viniciusvos0/teste_sx_negocios/releases/download/release-2024-12/banco_de_dados_prova_enem.sql
