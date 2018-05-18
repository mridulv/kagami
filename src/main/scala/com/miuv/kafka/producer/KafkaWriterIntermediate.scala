package com.miuv.kafka.producer

class KafkaWriterIntermediate(kafkaProducer: KafkaProducer) extends KafkaProducer.Publisher {

  subscribe(kafkaProducer)

  def replicateEntry(content: Array[Byte]): Unit = {
    publish(content)
  }
}
