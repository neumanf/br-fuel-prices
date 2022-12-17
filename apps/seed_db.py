import csv
from pymongo import MongoClient

client = MongoClient('mongodb://br_fuel_prices:br_fuel_prices@172.18.0.10:27017')
db = client.br_fuel_prices
fuel_prices = db.fuel_prices

# Insercao dos dados no banco
with open('/root/precos-semestrais-ca-2022-01.csv', 'r') as csvfile:
  header = ["regiao","estado","municipio","revenda","cnpj_da_revenda","nome_da_rua","numero_rua","complemento","bairro","cep","produto","data_da_coleta","valor_de_venda","valor_de_compra","unidade_de_medida","bandeira"]
  reader = csv.reader(csvfile, delimiter=';')
  next(reader)

  for row in reader:
    doc = {}
    for n in range(0, len(header)):
        doc[header[n]] = row[n]

    fuel_prices.insert_one(doc)
