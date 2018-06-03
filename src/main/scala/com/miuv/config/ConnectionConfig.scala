package com.miuv.config


case class ConnectionConfig(zookeeperConfig: ZookeeperConfig = ZookeeperConfig(),
                            kafkaConfig: KafkaConfig = KafkaConfig())

case class ZookeeperConfig(zookeeperConnectionString: String = "localhost:2181", connectionTimeout: Int = 200)
case class KafkaConfig(kafkaConnectionString: String = "localhost:9092")
