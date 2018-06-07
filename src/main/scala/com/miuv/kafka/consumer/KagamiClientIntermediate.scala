package com.miuv.kafka.consumer

import com.miuv.config.KafkaConfig
import com.miuv.core.KagamiClient
import com.miuv.kafka.{KafkaConsumerConfig, KafkaPartitionConfig}
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.{ConsumerInfo, SnapshotMetadata}
import com.miuv.util.{Logging, StringUtils}

class KagamiClientIntermediate(kafkaConsumerFactory: KafkaConsumerFactory,
                               token: Token,
                               kagamiClient: KagamiClient,
                               val snapshotMetadata: SnapshotMetadata,
                               kafkaConfig: KafkaConfig)
  extends KagamiKafkaConsumer.Subscriber with Logging {

  var kafkaConsumer: KagamiKafkaConsumer = _
  // Note(mridul, 2018-05-22) - This should ideally be a consumer for which we had stopped consuming, so that it does
  // not hinders the consumption of already existing consumers
  var consumerGroup: String = StringUtils.randomString(10)

  private def createKafkaConsumerConfig(kafkaPartitionConfig: Option[KafkaPartitionConfig]): KafkaConsumerConfig = {
    new KafkaConsumerConfig(topic = token, consumerGroup, kafkaPartitionConfig, kafkaConnectString = kafkaConfig.kafkaConnectionString)
  }

  def takeSnapshot(): SnapshotMetadata = {
    // Note(mridul, 2018-05-22) - This should be ensureStop, so that the thread gets blocked to make sure consumer
    // has stopped
    info(s"Stopping the consumer for token ${token}")
    kafkaConsumer.stop()
    val offset = kafkaConsumer.lastCommittedOffset
    val partitionNumber = kafkaConsumer.partitionNumber
    val path = kagamiClient.takeSnapshot(token)
    kafkaConsumer.start()
    info(s"Starting the consumer for token ${token} with details ${path} and ${offset} and ${consumerGroup}")
    SnapshotMetadata(Some(path), Some(ConsumerInfo(consumerGroup, partitionNumber, offset)))
  }

  def setup(): Unit = {
    snapshotMetadata.path.foreach(kagamiClient.loadSnapshot(token, _))
    val kafkaConsumerConfig = createKafkaConsumerConfig(
      snapshotMetadata.consumerInfoOpt.map(consumerInfo => {
        consumerGroup = consumerInfo.consumerGroup
        KafkaPartitionConfig(consumerInfo.offset, consumerInfo.partition)
      })
    )
    // Replicate the state of the consumer in case of already existing tokens for consumers when node has become unavailable
    kafkaConsumer = new KagamiKafkaConsumer(kafkaConsumerFactory, kafkaConsumerConfig, this)
    kafkaConsumer.start()
  }

  override def notify(pub: KagamiKafkaConsumer.Publisher, event: Array[Byte]): Unit = {
    kagamiClient.receiverReplicatedData(token, event)
  }
}
