package com.miuv.core.partitioner

import Partitioning.{Target, Token}
import TokenAssigner._
import com.miuv.curator.NodeId

class TokenAssigner(nodeId: NodeId, zookeeperPartitioningStore: ZookeeperPartitioningStore)
extends ClientPartitioner[String, String] {

  override def addToken(token: Token, numReplication: Int): Unit = {
    // add self as a target to token
    zookeeperPartitioningStore.withLock ({
      var partitioning = zookeeperPartitioningStore.load()
      partitioning.addToken(token, TokenMetadata(numReplication, Some(nodeId.nodeName), None, Array.empty))
      replicateUnderReplicatedToken(token, partitioning)
      zookeeperPartitioningStore.store(partitioning)
    })
  }
}

object TokenAssigner {
  private[partitioner] def replicateUnderReplicatedToken(token: Token, partitioning: Partitioning): Unit = {
    val metadata = partitioning.partitioning(token)
    (1 to (metadata.replication - metadata.secondaryTargets.length)).foreach(num => {
      val eligibleTarget = findEligibleTarget(partitioning, token)
      partitioning.addTargetToToken(token, eligibleTarget)
    })
  }

  private[partitioner] def findEligibleTarget(partitioning: Partitioning, token: Token): Target = {
    val inversePartitioning = partitioning.inversePartitioning()
    val eligibleTargets = inversePartitioning.filterNot(_._2.contains(token))
    eligibleTargets.toSeq.minBy(_._2.size)._1
  }

  private[partitioner] def findUnderReplicatedTokens(partitioning: Partitioning, currentTargets: Seq[Target]): Seq[Token] = {
    partitioning.partitioning.filter(token => token._2.replication > token._2.secondaryTargets.length).keys.toSeq
  }
}
