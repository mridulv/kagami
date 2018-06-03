package com.miuv.kafka.consumer

import com.miuv.config.KafkaConfig
import com.miuv.core.KagamiClient
import com.miuv.core.snapshot.SnapshotMetadata
import com.miuv.kafka.consumer._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.time.SpanSugar._
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class KagamiKafkaConsumerTest
  extends FlatSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Eventually {

  val kafkaConsumerFactory: KafkaConsumerFactory = mock[KafkaConsumerFactory]

  "KagamiKafkaConsumer" should "make sure that the data is read correctly" in {
    val kagamiClientIntermediate: MockKagamiClientIntermediate = new MockKagamiClientIntermediate(
      kafkaConsumerFactory,
      "topic",
      mock[KagamiClient],
      mock[SnapshotMetadata],
      mock[KafkaConfig]
    )

    val kafkaConsumer = mock[KafkaConsumer[Long, Array[Byte]]]
    val consumerRecords = mock[ConsumerRecords[Long, Array[Byte]]]
    var offset = 0
    val consumerRecordList = (1 to 10).map(elem => {
      val record = new ConsumerRecord[Long, Array[Byte]]("topic", 1, offset, 0, s"elem$offset".getBytes())
      offset += 1
      record
    }).toIterator.asJava
    val emptyList = List[ConsumerRecord[Long, Array[Byte]]]().asJava
    val topicAndPartition: mutable.Set[TopicPartition] = mutable.Set[TopicPartition](new TopicPartition("topic", 1))
    var answered: Boolean = false

    when(kafkaConsumerFactory.createConsumer(any())).thenReturn(kafkaConsumer)
    when(kafkaConsumer.poll(any())).thenReturn(consumerRecords)
    when(consumerRecords.count()).thenReturn(10)
    when(consumerRecords.iterator()).thenReturn(consumerRecordList)
    when(kafkaConsumer.assignment()).thenReturn(topicAndPartition.asJava)
    when(kafkaConsumer.committed(any())).thenReturn(getOffsetAndMetadata)

    val consumer = kagamiClientIntermediate.startConsumer()
    val expectedStrings = (1 to 10).map(elem => {
      new String(s"elem${elem}")
    }).sorted
    eventually(timeout(1.minute)) {
      kagamiClientIntermediate.str.sorted.foreach(expectedStrings.contains)
      consumer.lastCommittedOffset should be(10)
      consumer.partitionNumber should be(1)
    }
  }

  private def getOffsetAndMetadata: OffsetAndMetadata = {
    new OffsetAndMetadata(10L)
  }
}