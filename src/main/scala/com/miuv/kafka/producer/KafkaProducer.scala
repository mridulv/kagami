package com.miuv.kafka.producer

import java.util.Properties

import com.miuv.kafka.KafkaProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer => Producer}
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer, StringSerializer}

import scala.collection.mutable

class KafkaProducer(kafkaProducerConfig: KafkaProducerConfig)
  extends KafkaProducer.Subscriber {

  private var index = 0
  private val TOPIC = kafkaProducerConfig.topic //"random1"
  private val BOOTSTRAP_SERVERS = kafkaProducerConfig.kafkaConnectString //"localhost:9092"

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

  override def notify(pub: KafkaProducer.Publisher, event: Array[Byte]): Unit = {
    val record = new ProducerRecord[Long, Array[Byte]](TOPIC, index, event)
    val metadata = producer.send(record).get
    print("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n", record.key, record.value, metadata.partition, metadata.offset)
  }
}

object KafkaProducer {
  type Publisher = mutable.Publisher[Array[Byte]]
  type Subscriber = mutable.Subscriber[Array[Byte], Publisher]
}