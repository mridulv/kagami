package com.miuv.kafka.producer

class KagamiProducerIntermediate(kafkaProducer: KagamiKafkaProducer) extends KagamiKafkaProducer.Publisher {

  subscribe(kafkaProducer)

  def replicateEntry(content: Array[Byte]): Unit = {
    publish(content)
  }
}
