# Brazil Fuel Prices

Analysis of Brazil's fuel prices in the 1st half of 2022.

## Requirements

- Docker
- Docker Compose

> PS: Download [Hadoop](https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz), [Spark](https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz) and the [dataset](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/precos-semestrais-ca.zip) to the root folder of the project or enable the automatic downloads in the Dockerfile.

## Usage


```sh
# Run the cluster
docker-compose up --build

# Access the root node
docker exec -it node-master bash

# Execute a script with pyspark
spark-submit --packages org.mongodb.spark:mongo-spark-connector:10.0.5 <script-path>
```

## Credits

- [Docker Hadoop Cluster by cmdviegas](https://github.com/cmdviegas/docker-hadoop-cluster)
- [1o Sem 2022 - Combustíveis Automotivos by dados.gov.br](https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis-por-revenda)