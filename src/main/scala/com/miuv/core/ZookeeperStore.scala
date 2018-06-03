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

  private def getCurrentPartitioning(): String = {
    encoder.deserialize(curatorFramework.getData.forPath(path)).toString
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
    info(s"Current Partitioning is: ${getCurrentPartitioning()} as expected to: ${encoder.deserialize(encoded)}")
  }

  def load(): T = {
    val a = encoder.serialize(defaultEntry)
    if (noPartitioningExists()) {
      curatorFramework.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(path)
      curatorFramework.setData().forPath(path, encoder.serialize(defaultEntry))
    }

    info(s"Current Partitioning is: ${getCurrentPartitioning()}")

    val currentData = nodeCache.getCurrentData
    if (Option(currentData).isEmpty) nodeCache.rebuild()
    val data = nodeCache.getCurrentData.getData
    val res = encoder.deserialize(data)
    res
  }


  def noPartitioningExists(): Boolean = {
    curatorFramework.checkExists().forPath(path) == null
  }
}
