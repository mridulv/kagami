package com.miuv.kafka

class KafkaConsumerConfig(val topic: String, val consumerGroup: String, override val kafkaConnectString: String)
  extends KafkaConfig(kafkaConnectString)

class KafkaProducerConfig(val serverId: String, val topic: String, override val kafkaConnectString: String)
  extends KafkaConfig(kafkaConnectString)

class KafkaConfig(val kafkaConnectString: String)