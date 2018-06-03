package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class LeaderPartitionerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  val nodeId = new NodeId()

  "LeaderPartitioner" should "make sure that the tokens to target mapping is correct and balanced" in {
    val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(2, "topic-1", Some("target-1"), Array.empty),
      "token-2" -> TokenMetadata(2, "topic-1", Some("target-2"), Array.empty),
      "token-3" -> TokenMetadata(2, "topic-1", Some("target-3"), Array.empty)
    )
    when(zookeeperPartitioningStore.load()).thenReturn(new Partitioning(initialPartitioning))
    when(zookeeperPartitioningStore.lockPath).thenReturn("/new")
    val leaderPartitioner = new LeaderPartitioner(zookeeperPartitioningStore) with TestLockUtil

    when(zookeeperPartitioningStore.store(any())).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val partitioning = args(0).asInstanceOf[Partitioning]
        partitioning.partitioning.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3"))

        partitioning.partitioning("token-1").primaryTarget should be(Some("target-1"))
        partitioning.partitioning("token-2").primaryTarget should be(Some("target-2"))
        partitioning.partitioning("token-3").primaryTarget should be(Some("target-3"))

        partitioning.partitioning("token-1").secondaryTargets.toSeq.sorted should be(Seq("target-2", "target-3"))
        partitioning.partitioning("token-2").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-3"))
        partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))
      }
    })

    leaderPartitioner.doPartition(Seq("target-1", "target-2", "target-3"))
  }

  "LeaderPartitioner" should "make sure that the tokens to target mapping is correct for unavailable nodes" in {
    val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
    when(zookeeperPartitioningStore.lockPath).thenReturn("/new")
    val leaderPartitioner = new LeaderPartitioner(zookeeperPartitioningStore) with TestLockUtil

    val partitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(2, "topic-1", Some("target-1"), Array("target-2", "target-3")),
      "token-2" -> TokenMetadata(2, "topic-1", Some("target-2"), Array("target-3", "target-1")),
      "token-3" -> TokenMetadata(2, "topic-1", Some("target-3"), Array("target-1", "target-2"))
    )
    when(zookeeperPartitioningStore.load()).thenReturn(new Partitioning(partitioning))

    when(zookeeperPartitioningStore.store(any())).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val partitioning = args(0).asInstanceOf[Partitioning]
        partitioning.partitioning.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3"))

        partitioning.partitioning("token-1").primaryTarget should be(Some("target-1"))
        partitioning.partitioning("token-2").primaryTarget should be(Some("target-2"))
        partitioning.partitioning("token-3").primaryTarget should be(Some("target-3"))

        partitioning.partitioning("token-1").secondaryTargets.toSeq.sorted should be(Seq("target-2"))
        partitioning.partitioning("token-2").secondaryTargets.toSeq.sorted should be(Seq("target-1"))
        partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))
      }
    })

    leaderPartitioner.doPartition(Seq("target-1", "target-2"))
  }
}
