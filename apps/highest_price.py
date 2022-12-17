from pyspark.sql import functions as F
from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por produto
df = df.filter(df['produto'] == 'GASOLINA')

# Mapeia e converte valores do produto de string para float
df = df.rdd \
    .map(lambda x: (x.estado, x.municipio, x.produto, float(x.valor_de_venda.replace(",", ".")))) \
    .toDF(["estado", "municipio", "produto", "valor"])

# Obtem o valor maximo
df = df.groupBy("produto") \
    .agg(F.max("valor"), \
        F.expr("max_by(estado, valor) as estado"), \
        F.expr("max_by(municipio, valor) as municipio"))

df.show()