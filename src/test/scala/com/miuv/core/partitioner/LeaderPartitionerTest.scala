package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.mutable

class LeaderPartitionerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  val nodeId = new NodeId()
  val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
  val leaderPartitioner = new LeaderPartitioner(zookeeperPartitioningStore)

  "LeaderPartitioner" should "make sure that the tokens to target mapping is correct and balanced" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(1, Some("target-1"), None, Array.empty),
      "token-2" -> TokenMetadata(1, Some("target-2"), None, Array.empty),
      "token-3" -> TokenMetadata(1, Some("target-3"), None, Array.empty)
    )
    when(zookeeperPartitioningStore.load()).thenReturn(new Partitioning(initialPartitioning))

    when(zookeeperPartitioningStore.store(any())).thenAnswer(new Answer[_]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val partitioning = args(0).asInstanceOf[Partitioning]
        partitioning.partitioning.keys should be(Seq("token-1", "token-2", "token-3"))

        partitioning.partitioning("token-1").primaryTarget should be(Some("target-1"))
        partitioning.partitioning("token-1").primaryTarget should be(Some("target-2"))
        partitioning.partitioning("token-1").primaryTarget should be(Some("target-3"))

        partitioning.partitioning("token-1").secondaryTargets.toSeq.sorted should be(Seq("target-2", "target-3"))
        partitioning.partitioning("token-2").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-3"))
        partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))
      }
    })

    leaderPartitioner.doPartition(Seq("target-1", "target-2", "target-3"))
  }
}
