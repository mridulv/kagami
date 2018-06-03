package com.miuv.core.partitioner

import com.miuv.core.Encoder
import org.apache.curator.framework.CuratorFramework

class MockZookeeperPartitioningStore(initialPartitioning: Partitioning,
                                     override val curatorFramework: CuratorFramework,
                                     override val encoder: Encoder[Partitioning])
  extends ZookeeperPartitioningStore(curatorFramework, encoder)
    with TestLockUtil {

  var partitioning: Partitioning = null

  override def load(): Partitioning = {
    initialPartitioning
  }

  override def store(partitioning: Partitioning): Unit = {
    this.partitioning = partitioning
  }

}
