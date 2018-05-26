package com.miuv.kafka.consumer

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.SnapshotMetadata

class ReplicatorKafkaIntermediateFactory(connectionConfig: ConnectionConfig,
                                         replicatorClient: ReplicatorClient) {

  def createReplicatorKafkaClient(token: Token, snapshotMetadata: SnapshotMetadata): ReplicatorKafkaIntermediate = {
    new ReplicatorKafkaIntermediate(token, replicatorClient, snapshotMetadata, connectionConfig.kafkaConfig)
  }

}
