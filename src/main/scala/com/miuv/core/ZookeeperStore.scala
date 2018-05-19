package com.miuv.core

import com.miuv.util.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.zookeeper.CreateMode

trait ZookeeperStore[T] extends Logging {

  val encoder: Encoder[T]
  val curatorFramework: CuratorFramework
  val path: String
  val defaultEntry: T

  protected lazy val nodeCache: NodeCache = {
    val cache = new NodeCache(curatorFramework, path)
    cache.start()
    cache
  }

  def store(entry: T): Unit = {
    val encoded: Array[Byte] = encoder.serialize(entry)
    if (noPartitioningExists()) {
      curatorFramework.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path, encoded)
    } else {
      curatorFramework.setData()
        .forPath(path, encoded)
    }
  }

  def load(): T = {
    if (noPartitioningExists()) {
      curatorFramework.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path, encoder.serialize(defaultEntry))
      info(" No partitioning store found on Zookeeper. Creating an empty partitioning.")
    }

    val currentData = nodeCache.getCurrentData
    if (Option(currentData).isEmpty) nodeCache.rebuild()
    val res = encoder.deserialize(nodeCache.getCurrentData.getData)
    res
  }


  def noPartitioningExists(): Boolean = {
    curatorFramework.checkExists().forPath(path) == null
  }
}
