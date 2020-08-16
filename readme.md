# Apache Spark programming with Java
Example of Stand alone Spring Boot application with Java

## Build

```shellscript
mvn clean package
``` 

## Run

```shellscript
java -jar target/mapreduce-spark-0.0.1-SNAPSHOT.jar wc /user/student/shakespeare/tragedy/othello.txt /tmp/othello
java -jar target/mapreduce-spark-0.0.1-SNAPSHOT.jar fbc /user/student/airline/1987.csv /tmp/1987
```