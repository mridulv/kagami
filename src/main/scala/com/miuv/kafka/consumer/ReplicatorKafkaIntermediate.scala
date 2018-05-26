package com.miuv.kafka.consumer

import com.miuv.config.KafkaConfig
import com.miuv.kafka.KafkaConsumerConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.SnapshotMetadata

import scala.util.Random

class ReplicatorKafkaIntermediate(token: Token,
                                  replicatorClient: ReplicatorClient,
                                  val snapshotMetadata: SnapshotMetadata,
                                  kafkaConfig: KafkaConfig)
  extends KafkaConsumer.Subscriber {

  var kafkaConsumer: KafkaConsumer = _
  // Note(mridul, 2018-05-22) - This should ideally be a consumer for which we had stopped consuming, so that it does
  // not hinders the consumption of already existing consumers
  var consumerGroup: String = Random.nextString(10)

  private def createKafkaConsumerConfig(): KafkaConsumerConfig = {
    new KafkaConsumerConfig(topic = "random1", consumerGroup = consumerGroup, kafkaConnectString = kafkaConfig.kafkaConnectionString)
  }

  def takeSnapshot(): SnapshotMetadata = {
    // Note(mridul, 2018-05-22) - This should be ensureStop, so that the thread gets blocked to make sure consumer
    // has stopped
    kafkaConsumer.stop()
    val offset = kafkaConsumer.getOffset()
    val path = replicatorClient.takeSnapshot(token)
    kafkaConsumer.start()
    SnapshotMetadata(Some(path), offset, Some(consumerGroup))
  }

  def setup(): Unit = {
    val kafkaConsumerConfig = createKafkaConsumerConfig()
    snapshotMetadata.path.foreach(replicatorClient.loadSnapshot(token, _))
    // Replicate the state of the consumer in case of already existing tokens for consumers when node has become unavailable
    kafkaConsumer = new KafkaConsumer(kafkaConsumerConfig, this)
    kafkaConsumer.start()
  }

  override def notify(pub: KafkaConsumer.Publisher, event: Array[Byte]): Unit = {
    val payload = replicatorClient.deserializeRequest(token, event)
    replicatorClient.makeRequest(token, payload)
  }
}
