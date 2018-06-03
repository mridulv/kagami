package com.miuv.kafka.consumer

import java.util.{Collections, Properties}

import com.miuv.kafka.KafkaConsumerConfig
import com.miuv.util.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer}
import scala.collection.JavaConverters._

class KafkaConsumerFactory extends Logging {

  def createConsumer(kafkaConsumerConfig: KafkaConsumerConfig): KafkaConsumer[Long, Array[Byte]] = {
    val props = new Properties()
    setupBatchingAndCompression(props, kafkaConsumerConfig)
    val topic = kafkaConsumerConfig.topic
    val consumer = new KafkaConsumer[Long, Array[Byte]](props)
    consumer.subscribe(Collections.singletonList(topic))
    consumer
  }

  private def setupBatchingAndCompression(props: Properties, kafkaConsumerConfig: KafkaConsumerConfig) = {
    val BOOTSTRAP_SERVERS = kafkaConsumerConfig.kafkaConnectString
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerConfig.consumerGroup)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
  }

}
