package com.miuv.core.partitioner

import com.miuv.core.Encoder
import com.miuv.core.partitioner.Partitioning.Token
import org.apache.curator.framework.CuratorFramework
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ZookeeperPartitioningStoreTest extends FlatSpec with Matchers with MockitoSugar {
  val mockedCuratorFramework: CuratorFramework = mock[CuratorFramework]
  val mockedEncoder: Encoder[Partitioning] = mock[Encoder[Partitioning]]
  val zookeeperPartitioningStore = new ZookeeperPartitioningStore(mockedCuratorFramework, mockedEncoder) with ZookeeperStoreTestUtil with TestLockUtil
  val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
    "newToken" -> TokenMetadata(2, "topic-1", Some("target-1"), Array.empty)
  )
  val partitioningToBeLoaded = new Partitioning(initialPartitioning)
  partitioningToBeLoaded.addTargetToToken(token = "newToken", target = "newTarget")

  "ZookeeperPartitioningStore" should "listen to the partitioning and update the listeners" in {

    val testListener = new PartitioningListener {
      override def notifyListener(partitioning: Partitioning): Unit = {
        partitioning.partitioning.keys.toSeq.head should be ("newToken")
        partitioning.partitioning.values.toSeq.head.secondaryTargets.head should be ("newTarget")
      }
    }

    zookeeperPartitioningStore.listen(testListener)
    zookeeperPartitioningStore.store(partitioningToBeLoaded)
    zookeeperPartitioningStore.invokeListeners()
  }
}

