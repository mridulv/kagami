package com.miuv.kafka.producer

class KagamiProducerIntermediate(kafkaProducer: KagamiKafkaProducer) extends KagamiKafkaProducer.Publisher {

  subscribe(kafkaProducer)

  def sendDataForReplication(content: Array[Byte]): Unit = {
    publish(content)
  }
}
