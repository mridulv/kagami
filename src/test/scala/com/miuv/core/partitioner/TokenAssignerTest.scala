package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

class TokenAssignerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  val nodeId = new NodeId()
  val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]

  "TokenAssigner" should "make sure that the tokens are assigned carefully" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(1, Some("target-1"), Some("target-2"), Seq("target-1").toArray),
      "token-2" -> TokenMetadata(1, Some("target-2"), Some("target-3"), Seq("target-4").toArray),
      "token-3" -> TokenMetadata(1, Some("target-3"), Some("target-4"), Seq("target-2").toArray)
    )
    val tokenAssigner = new TokenAssigner(nodeId, zookeeperPartitioningStore)
    when(zookeeperPartitioningStore.load()).thenReturn(new Partitioning(initialPartitioning))

    import org.mockito.invocation.InvocationOnMock
    import org.mockito.stubbing.Answer

    when(zookeeperPartitioningStore.store(any())).thenAnswer(new Answer[_]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val partitioning = args(0).asInstanceOf[Partitioning]
        partitioning.partitioning.keys should be(Seq("token-1", "token-2", "token-3", "token-4"))
        partitioning.partitioning.values.map(_.primaryTarget).toSeq should be(Seq("target-1", "target-2", "target-3", "target-4"))
        partitioning.partitioning.values.map(_.snapshotReplica).toSeq should be(Seq("target-2", "target-3", "target-4"))

        partitioning.partitioning("token-4").secondaryTargets.toSeq.sorted should be(Seq("target-3"))
      }
    })

    tokenAssigner.addToken("token-4", 1)
  }

  "TokenAssigner" should "make sure that the tokens are assigned carefully" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(1, Some("target-1"), Some("target-2"), Seq("target-2").toArray),
      "token-2" -> TokenMetadata(1, Some("target-2"), Some("target-1"), Seq("target-1").toArray)
    )
    val tokenAssigner = new TokenAssigner(nodeId, zookeeperPartitioningStore)
    when(zookeeperPartitioningStore.load()).thenReturn(new Partitioning(initialPartitioning))

    import org.mockito.invocation.InvocationOnMock
    import org.mockito.stubbing.Answer

    when(zookeeperPartitioningStore.store(any())).thenAnswer(new Answer[_]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val partitioning = args(0).asInstanceOf[Partitioning]
        partitioning.partitioning.keys should be(Seq("token-1", "token-2", "token-3"))
        partitioning.partitioning.values.map(_.primaryTarget).toSeq should be(Seq("target-1", "target-2"))
        partitioning.partitioning.values.map(_.snapshotReplica).toSeq should be(Seq("target-1", "target-2"))

        partitioning.partitioning("token-3").snapshotReplica.isEmpty should be true
        partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))
      }
    })

    tokenAssigner.addToken("token-3", 2)
  }
}
