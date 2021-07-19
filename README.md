# DSID - EP2 - Análise de dados com Spark

Esse é o projeto para o EP2 da disciplina ACH2147 - Desenvolvimento de Sistemas Distribuídos

## Pré-requisitos
 - JDK 11 (testado com a última versão da JDK11 OpenJDK encontrada em: https://jdk.java.net/archive/)
 - Credenciais de uma conta da AWS (como estamos acessando dados públicos, 
   é necessário apenas um usuário criado para autenticação. Não é feito nenhum tipo de cobrança com os testes do EP)
 - [spark-3.1.2-bin-hadoop3.2](https://spark.apache.org/downloads.html)
 - Docker (para subir a imagem do mongodb)

## Setup
- Instale as dependências:
`./mvnw install`
  
- Suba a instância do mongo configurada no docker-compose
`docker-compose up -d mongo`

- Copie o arquivo de propriedades (`src/main/resources/application.properties.sample`), retirando o `.sample`.
  e ajustando as propriedades de acordo com a sua máquina
```
# Caminho da pasta onde o spark está instalado
spark.home=/home/renan/Documents/dev/spark-3.1.2-bin-hadoop3.2

# Caminho do jar contendo as funções de agregação, gerado em aggregation-functions/target/aggregation-functions-1.0.0-SNAPSHOT.jar
spark.aggregations.jarPath=/home/renan/Documents/dev/dsid-ep2/aggregation-functions/target/aggregation-functions-1.0.0-SNAPSHOT.jar

spark.datasource.baseUrl=s3a://aws-gsod

# Credenciais da AWS
spark.aws.access.key=MY_KEY
spark.aws.secret.key=MY_SECRET

# Configurações do mongodb
quarkus.mongodb.connection-string=mongodb://root:example@localhost:27017/
quarkus.mongodb.credentials.auth-source=admin
```
