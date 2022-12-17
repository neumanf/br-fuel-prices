from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por produto
df = df.filter(df['Produto'] == 'GASOLINA')

# Mapeia e converte valores do produto de string para float
df = df.rdd.map(lambda x: (x.Produto, float(x.Valor_de_Venda.replace(",", ".")))).toDF(["Produto", "Valor"])

# Obtem o valor maximo
df = df.groupBy("Produto").max("Valor")

df.show()