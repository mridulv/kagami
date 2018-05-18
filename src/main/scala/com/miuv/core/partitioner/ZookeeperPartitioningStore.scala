package com.miuv.core.partitioner

import com.miuv.core.{Encoder, ZookeeperStore}
import com.miuv.curator.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.zookeeper.CreateMode

class ZookeeperPartitioningStore(override val curatorFramework: CuratorFramework,
                                 override val encoder: Encoder[Partitioning])
  extends ZookeeperStore[Partitioning]
  with ZookeeperLockUtil
    with Logging {

  def listen(partitioningListener: PartitioningListener): Unit = {
    nodeCache.getListenable.addListener(new NodeCacheListener {
      override def nodeChanged(): Unit = {
        withLock({
          partitioningListener.notify(load())
        })
      }
    })
  }

  override val path: String = "/compressed-partitioning/partitioning"
  override val lockPath: String = path
  override val defaultEntry: Partitioning = new Partitioning()
}