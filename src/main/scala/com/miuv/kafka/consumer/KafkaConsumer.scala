package com.miuv.kafka.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.{KafkaConsumer => Consumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer, StringDeserializer}
import java.util.Collections
import scala.collection.JavaConverters._

import com.miuv.kafka.KafkaConsumerConfig
import com.miuv.util.StartStoppable

import scala.collection.mutable

class KafkaConsumer(kafkaConsumerConfig: KafkaConsumerConfig,
                    replicatorKafkaIntermediate: ReplicatorKafkaIntermediate)
  extends KafkaConsumer.Publisher with StartStoppable {

  subscribe(replicatorKafkaIntermediate)

  private var continue = true
  private val TOPIC = kafkaConsumerConfig.topic
  private val BOOTSTRAP_SERVERS = kafkaConsumerConfig.kafkaConnectString

  private val kafkaBasicConsumer = {
    val props = new Properties()
    setupBatchingAndCompression(props)
    val consumer = new Consumer[Long, Array[Byte]](props)
    consumer.subscribe(Collections.singletonList(TOPIC))
    consumer
  }

  private def setupBatchingAndCompression(props: Properties) = {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerConfig)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
  }

  def getOffset(): Long = {
    val partitions = kafkaBasicConsumer.assignment()
    kafkaBasicConsumer.endOffsets(partitions).values().asScala.head
  }

  @throws[InterruptedException]
  def runConsumer(): Unit = {
    val consumer = kafkaBasicConsumer
    val giveUp = 100
    var noRecordsCount = 0
    while (continue) {
      val consumerRecords = consumer.poll(1000)
      if (consumerRecords.count == 0) {
        noRecordsCount += 1
      } else {
        val records = consumerRecords.iterator()
        while(records.hasNext) {
          val singleRecord = records.next()
          publish(singleRecord.value())
          print(singleRecord.key() + " it is here " + singleRecord.value())
        }
        consumer.commitAsync()
      }
    }
    consumer.close()
    print("DONE")
  }

  def init(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        runConsumer()
      }
    }).start()
  }

  override def doStart(): Unit = {
    continue = true
  }

  override def doStop(): Unit = {
    continue = false
  }
}

object KafkaConsumer {
  type Publisher = mutable.Publisher[Array[Byte]]
  type Subscriber = mutable.Subscriber[Array[Byte], Publisher]
}