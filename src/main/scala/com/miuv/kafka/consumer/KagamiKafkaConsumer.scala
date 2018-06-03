package com.miuv.kafka.consumer

import scala.collection.JavaConverters._
import com.miuv.kafka.KafkaConsumerConfig
import com.miuv.util.{Logging, StartStoppable}
import org.apache.kafka.common.TopicPartition

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
    var elemsRead = 0
    var alreadySet = false
    while (continue) {
      val assignments = consumer.assignment()
      if (!alreadySet && assignments.asScala.nonEmpty) {
        val topic = kafkaConsumerConfig.topic
        consumer.seekToBeginning(assignments)
        kafkaConsumerConfig.kafkaPartitionConfig.foreach(consumerInfo => {
          info(s"Initializing Consumer with Topic $topic and partition ${consumerInfo.partition} and offset ${consumerInfo.offset} and ${consumer.assignment().asScala.map(_.partition())}")
          consumer.seek(new TopicPartition(topic, consumerInfo.partition), consumerInfo.offset)
        })
        alreadySet = true
      }
      val consumerRecords = consumer.poll(1000)
      if (consumerRecords.count == 0 || assignments.asScala.isEmpty) {
        noRecordsCount += 1
      } else {
        val records = consumerRecords.iterator()
        while(records.hasNext) {
          val singleRecord = records.next()
          elemsRead += 1
          println(s"We are reading and publishing bytes for token ${kafkaConsumerConfig.topic} " +
            s"${consumer.position(assignments.asScala.head)} and ${consumer} and ${singleRecord.key()} and ${new String(singleRecord.value())}")
          consumer.commitSync()
          publish(singleRecord.value())
        }
        reinitializeOffsetAndPartition(assignments.asScala.toSet)
      }
    }
    consumer.close()
  }

  private def reinitializeOffsetAndPartition(assignments: Set[TopicPartition]): Unit = {
    try {
        val offsets = kafkaBasicConsumer.committed(assignments.head)
        partitionNumber = assignments.head.partition()
        lastCommittedOffset = offsets.offset()
    } catch {
      case e: Exception => {
        error(s"Reinitialization Failed ${assignments} and ${kafkaBasicConsumer.committed(assignments.head)}" + e.getMessage)
        lastCommittedOffset = -1
      }
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