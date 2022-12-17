from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por municipio e produto
df = df.filter((df['municipio'] == 'NATAL') & (df['produto'] == 'GASOLINA'))

# Mapeia e converte valores do produto de string para float
df = df.rdd.map(lambda x: (x.municipio, float(x.valor_de_venda.replace(",", ".")))).toDF(["municipio", "valor"])

# Obtem a media dos valores para o municipio em questao
df = df.groupBy("municipio").avg("valor")

df.show()