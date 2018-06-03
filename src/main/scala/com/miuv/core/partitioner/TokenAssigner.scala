package com.miuv.core.partitioner

import Partitioning.{Target, Token}
import com.miuv.curator.NodeId
import com.miuv.util.Logging

class TokenAssigner(nodeId: NodeId, zookeeperPartitioningStore: ZookeeperPartitioningStore)
  extends ClientPartitioner[String, String] {

  import TokenAssigner._

  override def addToken(token: Token, numReplication: Int): Unit = {
    // add self as a target to token
    info(s"We are going to add a token: $token with replication $numReplication")
    zookeeperPartitioningStore.withLock ({
      info(s"2 We are going to add a token: $token with replication $numReplication")
      var partitioning = zookeeperPartitioningStore.load()
      partitioning.addToken(token, TokenMetadata(numReplication, token, Some(nodeId.nodeName), Array.empty))
      val underReplicatedTokens = findUnderReplicatedTokens(partitioning)
      info(s"this is while listening new 2 We are having partitioning ${underReplicatedTokens.mkString(",")}")
      underReplicatedTokens.foreach(replicateUnderReplicatedToken(_, partitioning, _ => true))
      zookeeperPartitioningStore.store(partitioning)
    })
  }
}

object TokenAssigner extends Logging {
  private[partitioner] def replicateUnderReplicatedToken(token: Token, partitioning: Partitioning, filterTargetFunc: Target => Boolean): Unit = {
    val metadata = partitioning.partitioning(token)
    (1 to (metadata.replication - metadata.secondaryTargets.length)).foreach(num => {
      val eligibleTarget = findEligibleTarget(partitioning, token, filterTargetFunc)
      eligibleTarget.foreach(partitioning.addTargetToToken(token, _))
    })
  }

  private[partitioner] def findEligibleTarget(partitioning: Partitioning, token: Token, fn: Target => Boolean): Option[Target] = {
    val inversePartitioning = partitioning.inversePartitioning()
    val eligibleTargets = inversePartitioning.filterNot(_._2.contains(token))
    val eligibleLiveTargets = eligibleTargets.filter(elem => fn(elem._1))
    if (eligibleLiveTargets.nonEmpty) {
      Some(eligibleLiveTargets.toSeq.minBy(_._2.size)._1)
    } else {
      None
    }
  }

  private[partitioner] def findUnderReplicatedTokens(partitioning: Partitioning): Seq[Token] = {
    partitioning.partitioning.filter(token => token._2.replication > token._2.secondaryTargets.length).keys.toSeq
  }
}
