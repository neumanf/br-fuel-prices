from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por produto
df = df.filter(df['produto'] == 'GASOLINA')

# Mapeia e converte valores do produto de string para float
df = df.rdd.map(lambda x: (x.regiao, float(x.valor_de_venda.replace(",", ".")))).toDF(["regiao", "valor"])

# Obtem o valor minimo por regiao
df = df.groupBy("regiao").min("valor")

df.show()