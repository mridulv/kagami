package com.miuv.kafka.consumer

import scala.collection.JavaConverters._
import com.miuv.kafka.KafkaConsumerConfig
import com.miuv.util.{Logging, StartStoppable}

import scala.collection.mutable

class KagamiKafkaConsumer(kafkaConsumerFactory: KafkaConsumerFactory,
                          kafkaConsumerConfig: KafkaConsumerConfig,
                          replicatorKafkaIntermediate: KagamiClientIntermediate)
  extends KagamiKafkaConsumer.Publisher with StartStoppable
    with Logging {

  subscribe(replicatorKafkaIntermediate)

  private var continue = true

  var lastCommittedOffset: Long = 0L
  var partitionNumber: Int = 0

  private val kafkaBasicConsumer = {
    kafkaConsumerFactory.createConsumer(kafkaConsumerConfig)
  }

  def runConsumer(): Unit = {
    val consumer = kafkaBasicConsumer
    var noRecordsCount = 0
    while (continue) {
      val consumerRecords = consumer.poll(1000)
      if (consumerRecords.count == 0) {
        noRecordsCount += 1
      } else {
        val records = consumerRecords.iterator()
        while(records.hasNext) {
          val singleRecord = records.next()
          println(s"We are reading and publishing bytes for token ${kafkaConsumerConfig.topic} and ${lastCommittedOffset} and ${singleRecord.key()} and ${singleRecord.value()}")
          publish(singleRecord.value())
        }
        reinitializeOffsetAndPartition()
        consumer.commitSync()
      }
    }
    consumer.close()
  }

  private def reinitializeOffsetAndPartition(): Unit = {
    try {
      val assignments = kafkaBasicConsumer.assignment()
      val offsets = kafkaBasicConsumer.committed(assignments.asScala.head)
      partitionNumber = assignments.asScala.head.partition()
      lastCommittedOffset = offsets.offset()
    } catch {
      case e: Exception => error(e.getMessage)
    }
  }

  init()

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

object KagamiKafkaConsumer {
  type Publisher = mutable.Publisher[Array[Byte]]
  type Subscriber = mutable.Subscriber[Array[Byte], Publisher]
}