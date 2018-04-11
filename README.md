# SparkKafka10Test
#### build
```
mvn clean package
```

### run 
```
export SPARK_MAJOR_VERSION=2
spark-submit --master yarn-client --num-executors 1 --executor-memory 1g  --class com.rajkrrsingh.spark.kafka10.Kafka10WordCount SparkKafka10Test-1.0-SNAPSHOT-jar-with-dependencies.jar `hostname`:6667 test-group1 kafkatopic
```

