package com.miuv.core

import com.miuv.core.partitioner.Partitioning.{Target, Token}
import com.miuv.core.partitioner.ZookeeperPartitioningStore
import com.miuv.kafka.consumer.TokenNotFoundException
import com.miuv.kafka.producer.KagamiProducerIntermediate

trait ReplicatorWriter extends BasicReplicator {
  def add(token: Token, numReplication: Int): KagamiProducerIntermediate
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