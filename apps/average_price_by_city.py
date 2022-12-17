from init_spark import init_spark

spark = init_spark()

df = spark.read.format("mongodb").load()

# Filtra por municipio e produto
df = df.filter((df['Municipio'] == 'NATAL') & (df['Produto'] == 'GASOLINA'))

# Mapeia e converte valores do produto de string para float
df = df.rdd.map(lambda x: (x.Municipio, float(x.Valor_de_Venda.replace(",", ".")))).toDF(["Municipio", "Valor"])

# Obtem a media dos valores para o municipio em questao
df = df.groupBy("Municipio").avg("Valor")

df.show()