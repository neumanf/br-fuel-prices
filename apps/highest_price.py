from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por produto
df = df.filter(df['produto'] == 'GASOLINA')

# Mapeia e converte valores do produto de string para float
df = df.rdd.map(lambda x: (x.produto, float(x.valor_de_venda.replace(",", ".")))).toDF(["produto", "valor"])

# Obtem o valor maximo
df = df.groupBy("produto").max("valor")

df.show()