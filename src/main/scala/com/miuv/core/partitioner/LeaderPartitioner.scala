package com.miuv.core.partitioner

import TokenAssigner._
import com.miuv.core.snapshot.ZookeeperSnapshotMetadataStore
import org.apache.curator.framework.CuratorFramework

class LeaderPartitioner(zookeeperPartitioningStore: ZookeeperPartitioningStore)
  extends ZookeeperLockUtil {

  override val curatorFramework: CuratorFramework = zookeeperPartitioningStore.curatorFramework
  override val lockPath: String = zookeeperPartitioningStore.lockPath

  def doPartition(currentTargets: Seq[String]): Unit = {
    withLock({
      val currentPartitioning = zookeeperPartitioningStore.load()
      val underReplicatedTokens = findUnderReplicatedPartition(currentPartitioning, currentTargets)
      underReplicatedTokens.foreach(replicateUnderReplicatedToken(_, currentPartitioning))
      zookeeperPartitioningStore.store(currentPartitioning)
    })
  }
}
