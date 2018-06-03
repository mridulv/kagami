package com.miuv.kafka.producer

import java.util.Properties

import com.miuv.kafka.KafkaProducerConfig
import com.miuv.util.Logging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer => Producer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer, StringSerializer}

import scala.collection.mutable

class KagamiKafkaProducer(kafkaProducerConfig: KafkaProducerConfig)
  extends KagamiKafkaProducer.Subscriber
    with Logging {

  private var index = 0
  private val TOPIC = kafkaProducerConfig.topic
  private val BOOTSTRAP_SERVERS = kafkaProducerConfig.kafkaConnectString

  private val producer = {
    val props = new Properties()
    setupBatchingAndCompression(props)
    new Producer[Long, Array[Byte]](props)
  }

  private def setupBatchingAndCompression(props: Properties) = {
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProducerConfig.serverId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
  }

  override def notify(pub: KagamiKafkaProducer.Publisher, event: Array[Byte]): Unit = {
    val record = new ProducerRecord[Long, Array[Byte]](TOPIC, index, event)
    val metadata = producer.send(record).get
    info(s"sent record(key=${record.key} value=${new String(record.value)}) " + s"meta(partition=${metadata.partition}, offset=${metadata.offset})")
  }
}

object KagamiKafkaProducer {
  type Publisher = mutable.Publisher[Array[Byte]]
  type Subscriber = mutable.Subscriber[Array[Byte], Publisher]
}