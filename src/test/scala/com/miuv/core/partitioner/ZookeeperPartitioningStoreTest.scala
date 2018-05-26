package com.miuv.core.partitioner

import com.miuv.core.Encoder
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar

class ZookeeperPartitioningStoreTest extends FlatSpec with Matchers with MockitoSugar {
  val mockedCuratorFramework: CuratorFramework = mock[CuratorFramework]
  val mockedEncoder: Encoder[Partitioning] = mock[Encoder[Partitioning]]
  val zookeeperPartitioningStore = new ZookeeperPartitioningStore(mockedCuratorFramework, mockedEncoder)
  val partitioningToBeLoaded = new Partitioning()
  partitioningToBeLoaded.addTargetToToken(token = "newToken", target = "newTarget")

  "ZookeeperPartitioningStore" should "listen to the partitioning and update the listeners" in {

    val testListener = new PartitioningListener {
      override def notifyListener(partitioning: Partitioning): Unit = {
        partitioning.partitioning.keys.toSeq.head should be ("newToken")
        partitioning.partitioning.values.toSeq.head should be ("newTarget")
      }
    }

    zookeeperPartitioningStore.listen(testListener)
    zookeeperPartitioningStore.store(partitioningToBeLoaded)
  }
}

