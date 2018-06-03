package com.miuv.kafka

case class KafkaPartitionConfig(offset: Long, partition: Int)

class KafkaConsumerConfig(val topic: String, val consumerGroup: String, val kafkaPartitionConfig: Option[KafkaPartitionConfig], override val kafkaConnectString: String)
  extends KafkaConfig(kafkaConnectString)

class KafkaProducerConfig(val serverId: String, val topic: String, override val kafkaConnectString: String)
  extends KafkaConfig(kafkaConnectString)

class KafkaConfig(val kafkaConnectString: String)