import sys
import pandas as pd # para manipulação de dados
import boto3   # Importando o boto3, ele permite que os desenvolvedores Python interajam com serviços AWS, como S3.
from io import BytesIO  # Permite tratar bytes em memória como um "arquivo virtual" para leitura.
import psycopg2 # Para conectar e interagir com o banco de dados Redshift
import sys # Acesso a funções e variáveis do sistema.
from awsglue.utils import getResolvedOptions # Recupera parâmetros definidos no GlueJob.


#Primeiramente, retiramos nossa base do S3 com o boto3
s3 = boto3.client('s3')

##Passamos a inforamação necessária para conseguirmos acessar o bucket e o seu arquivo
bucket = "provadataops2023"
object_key = "shopping_trendsCsv.csv"

#criamos uma váriavel que busca o arquivo do s3
pegaarquivo = s3.get_object(Bucket=bucket, Key=object_key)



# Lendo o conteúdo do objeto obtido em um buffer temporário.
buffers = BytesIO(pegaarquivo['Body'].read())
####
# Lendo o buffer diretamente com o pandas para obter um DataFrame.
df = pd.read_csv(buffers)


###DADOS LIDOS COM SUCESSO.
#Como nosso trabalho estava dando muito erro, preferimos ir testando linha a linha do código e ir sinalizando quais areas tivemos erros
import psycopg2


# Tivemos problema na conexão com o Redshift pq era necessário retirar a porta antes de passar o hostname para acessar o mesmo
dbname = 'dev'
host = 'redshift-cluster-prova.ccworzdrdepf.us-east-1.redshift.amazonaws.com'
port = '5439'
user = 'awsuser'
password = 'Abcd1234'

# Estabelecendo a conexão
conn_string = f"dbname='{dbname}' port='{port}' user='{user}' password='{password}' host='{host}'"
conn = psycopg2.connect(conn_string)


# Para conseguirmos criar consultas no SQL, precisavamos criar um cursor para conseguir acessa-las e criamos uma linha de código caso nossa tabela já existisse.
cur = conn.cursor()
drop_table_query = """
DROP TABLE IF EXISTS ETLshopping;
"""


# Executando o comando de exclusão
cur = conn.cursor()
cur.execute(drop_table_query)


# Criação da tabela
create_table_query = """
CREATE TABLE IF NOT EXISTS ETLshopping (
    customer_id INT,
    age INT,
    gender VARCHAR(20),
    item_purchased VARCHAR(50),
    purchase_amount_usd INT,
    location VARCHAR(100),
    season VARCHAR(20),
    payment_method VARCHAR(50)
);
"""
cur.execute(create_table_query)
conn.commit()

# Extrair valores do DataFrame
values = [tuple(row) for row in df[['Customer ID', 'Age', 'Gender', 'Item Purchased', 'Purchase Amount (USD)', 'Location', 'Season', 'Payment Method']].values]


# Montar a consulta SQL para inserção
insert_query = """
    INSERT INTO ETLshopping (
        customer_id, age, gender, item_purchased, purchase_amount_usd, location, season, payment_method
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
cur.executemany(insert_query, values)
conn.commit()

# Atualizar os valores da coluna "gender" para binário
update_gender_query = """
UPDATE ETLshopping
SET gender = CASE
    WHEN gender = 'Male' THEN 1
    WHEN gender = 'Female' THEN 0
    ELSE NULL
END;
"""
cur.execute(update_gender_query)
conn.commit()

# Fechando o cursor e a conexão com o banco de dados, para liberar os recursos.
cur.close()
conn.close()
