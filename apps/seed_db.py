import csv
from pymongo import MongoClient

client = MongoClient('mongodb://br_fuel_prices:br_fuel_prices@172.18.0.10:27017')
db = client.br_fuel_prices
fuel_prices = db.fuel_prices

# Insercao dos dados no banco
with open('/root/precos-semestrais-ca-2022-01.csv', 'r') as csvfile:
  header = ["Regiao","Estado","Municipio","Revenda","CNPJ_da_Revenda","Nome_da_Rua","Numero_Rua","Complemento","Bairro","Cep","Produto","Data_da_Coleta","Valor_de_Venda","Valor_de_Compra","Unidade_de_Medida","Bandeira"]
  reader = csv.reader(csvfile, delimiter=';')
  next(reader)

  for row in reader:
    doc = {}
    for n in range(0, len(header)):
        doc[header[n]] = row[n]

    fuel_prices.insert_one(doc)
