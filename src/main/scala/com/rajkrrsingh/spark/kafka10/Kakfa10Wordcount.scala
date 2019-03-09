package com.rajkrrsingh.spark.kafka10
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable


object Kafka10WordCount {

  def main(argv: Array[String]): Unit = {

    if (argv.length != 3) {
      System.err.println(s"Kafka10WordCount: bootstrap_server_list group_id topic_list")
      System.exit(-1)
    }

    val Array(brokers, groupId, topics) = argv
    val kafkaParams = new mutable.HashMap[String, Object]()
    kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    val conf = new SparkConf().setAppName("Kafka10wordcount").setMaster("local[*]")

    //Configure batch of 30 seconds
    val sparkStreamingContext = new StreamingContext(conf, Durations.seconds(30))

    //Configure Spark to listen messages in topic test
    val topicSet = topics.split(",").map(_.trim).filter(!_.isEmpty).toSet
    // Read value of each message from Kafka and return it
    val messageStream = KafkaUtils.createDirectStream(sparkStreamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams))
    val lines = messageStream.map(consumerRecord => consumerRecord.value().asInstanceOf[String])

    // Break every message into words and return list of words
    val words = lines.flatMap(_.split(" "))

    // Take every word and return Tuple with (word,1)
    val wordMap = words.map(word => (word, 1))

    // Count occurance of each word
    //val wordCount = wordMap.reduceByKey((first, second) => first + second)

    //Print the word count
    //wordCount.print()
    wordMap.print()

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}