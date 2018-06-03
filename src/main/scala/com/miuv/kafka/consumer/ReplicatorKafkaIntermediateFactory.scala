package com.miuv.kafka.consumer

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.SnapshotMetadata

class ReplicatorKafkaIntermediateFactory(kafkaConsumerFactory: KafkaConsumerFactory,
                                         connectionConfig: ConnectionConfig,
                                         replicatorClient: KagamiClient) {

  def createReplicatorKafkaClient(token: Token, snapshotMetadata: SnapshotMetadata): KagamiClientIntermediate = {
    new KagamiClientIntermediate(kafkaConsumerFactory, token, replicatorClient, snapshotMetadata, connectionConfig.kafkaConfig)
  }

}
