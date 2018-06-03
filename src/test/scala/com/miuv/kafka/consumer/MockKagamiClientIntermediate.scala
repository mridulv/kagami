package com.miuv.kafka.consumer

import com.miuv.config.KafkaConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.SnapshotMetadata
import com.miuv.kafka.KafkaConsumerConfig
import org.scalatest.Matchers

import scala.collection.mutable.ArrayBuffer

class MockKagamiClientIntermediate(kafkaConsumerFactory: KafkaConsumerFactory,
                                   token: Token,
                                   kagamiClient: KagamiClient,
                                   snapshotMetadata: SnapshotMetadata,
                                   kafkaConfig: KafkaConfig)
  extends KagamiClientIntermediate(kafkaConsumerFactory, token, kagamiClient, snapshotMetadata, kafkaConfig)
    with KagamiKafkaConsumer.Subscriber with Matchers {

  var p = 0
  var str: ArrayBuffer[String] = ArrayBuffer[String]()

  def startConsumer(): KagamiKafkaConsumer = {
    val kafkaConsumerConfig = new KafkaConsumerConfig("topic", "consumer1", None, "localhost:9092")
    val kagamiKafkaConsumer = new KagamiKafkaConsumer(kafkaConsumerFactory, kafkaConsumerConfig, this)
    kagamiKafkaConsumer.start()
    kagamiKafkaConsumer
  }

  override def notify(pub: KagamiKafkaConsumer.Publisher, event: Array[Byte]): Unit = {
    val actualString = new String(event)
    p += 1
    str.append(actualString)
  }

}
