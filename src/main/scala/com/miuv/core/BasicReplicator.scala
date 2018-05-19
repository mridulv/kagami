package com.miuv.core

import com.miuv.core.partitioner.Partitioning.{Target, Token}
import com.miuv.core.partitioner.ZookeeperPartitioningStore
import com.miuv.kafka.consumer.{ReplicatorClient, TokenNotFoundException}
import com.miuv.kafka.producer.KafkaWriterIntermediate

trait ReplicatorWriter extends BasicReplicator {
  def add(token: Token, numReplication: Int): KafkaWriterIntermediate
}

trait ReplicatorReader extends BasicReplicator {
}

trait BasicReplicator {
  protected val zookeeperPartitioningStore: ZookeeperPartitioningStore
  def getPrimary(token: Token): Target = {
    val partitioning = zookeeperPartitioningStore.load()
    val tokenPartitioning = partitioning.partitioning.getOrElse(token, throw TokenNotFoundException("Token is not found"))
    tokenPartitioning.secondaryTargets.headOption.getOrElse(throw new Exception("No Targets found for this token"))
  }
}