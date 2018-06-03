package com.miuv.core.partitioner

import TokenAssigner._
import com.miuv.core.snapshot.ZookeeperSnapshotMetadataStore
import com.miuv.util.Logging
import org.apache.curator.framework.CuratorFramework

class LeaderPartitioner(zookeeperPartitioningStore: ZookeeperPartitioningStore)
  extends ZookeeperLockUtil with Logging {

  override val curatorFramework: CuratorFramework = zookeeperPartitioningStore.curatorFramework
  override val lockPath: String = zookeeperPartitioningStore.lockPath

  def doPartition(currentTargets: Seq[String]): Unit = {
    withLock({
      val currentPartitioning = zookeeperPartitioningStore.load()
      val partitioningAfterUnavailableNodes = removeUnavailableNodesFromPartitioning(currentPartitioning, currentTargets)
      val underReplicatedTokens = findUnderReplicatedTokens(partitioningAfterUnavailableNodes)
      underReplicatedTokens.foreach(replicateUnderReplicatedToken(_, partitioningAfterUnavailableNodes, currentTargets.contains))
      zookeeperPartitioningStore.store(partitioningAfterUnavailableNodes)
    })
  }

  private def removeUnavailableNodesFromPartitioning(partitioning: Partitioning, availableNodes: Seq[String]): Partitioning = {
    val p = partitioning.partitioning
    val newPartitioning = p.map(tokenWithMetadata => {
      val token = tokenWithMetadata._1
      val metadata = tokenWithMetadata._2
      val secondaryTargets = metadata.secondaryTargets.toSeq.filter(availableNodes.contains).toArray
      token -> TokenMetadata(metadata.replication, metadata.topic, metadata.primaryTarget, secondaryTargets)
    })
    info(s"New Partitioning is: ${newPartitioning} and interesting bit is ${availableNodes}")
    new Partitioning(newPartitioning)
  }
}
