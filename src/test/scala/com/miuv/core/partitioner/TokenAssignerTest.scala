package com.miuv.core.partitioner

import com.miuv.core.Encoder
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import org.apache.curator.framework.CuratorFramework
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class TokenAssignerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  val nodeId = new NodeId("target-4")
  val curatorFramework: CuratorFramework = mock[CuratorFramework]
  val encoder: Encoder[Partitioning] = mock[Encoder[Partitioning]]

  "TokenAssigner" should "make sure that the tokens are assigned carefully" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(1, "token-1", Some("target-1"), Seq("target-1").toArray),
      "token-2" -> TokenMetadata(1, "token-2", Some("target-2"), Seq("target-4").toArray),
      "token-3" -> TokenMetadata(1, "token-3", Some("target-3"), Seq("target-2").toArray)
    )
    val zookeeperPartitioningStore: MockZookeeperPartitioningStore = new MockZookeeperPartitioningStore(
      new Partitioning(initialPartitioning),
      curatorFramework,
      encoder
    )
    val tokenAssigner = new TokenAssigner(nodeId, zookeeperPartitioningStore)
    tokenAssigner.addToken("token-4", 1)

    val partitioning = zookeeperPartitioningStore.partitioning
    partitioning.partitioning.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3", "token-4").sorted)
    partitioning.partitioning.values.flatten(_.primaryTarget).toSeq.sorted should be(Seq("target-1", "target-2", "target-3", "target-4").sorted)
    partitioning.partitioning.values.map(_.topic).toSeq.sorted should be(Seq("token-1", "token-2", "token-3", "token-4").sorted)

    partitioning.partitioning("token-4").secondaryTargets.toSeq.sorted should be(Seq("target-3"))
  }

  "TokenAssigner" should "make sure too that the tokens are assigned carefully" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(1, "token-1", Some("target-1"), Seq("target-2").toArray),
      "token-2" -> TokenMetadata(1, "token-2", Some("target-2"), Seq("target-1").toArray)
    )
    val zookeeperPartitioningStore: MockZookeeperPartitioningStore = new MockZookeeperPartitioningStore(
      new Partitioning(initialPartitioning),
      curatorFramework,
      encoder
    )
    val tokenAssigner = new TokenAssigner(nodeId, zookeeperPartitioningStore)
    tokenAssigner.addToken("token-3", 2)

    val partitioning = zookeeperPartitioningStore.partitioning
    partitioning.partitioning.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3").sorted)
    partitioning.partitioning.values.flatten(_.primaryTarget).toSeq.sorted should be(Seq("target-1", "target-2", "target-4").sorted)
    partitioning.partitioning.values.map(_.topic).toSeq.sorted should be(Seq("token-1", "token-2", "token-3").sorted)

    partitioning.partitioning("token-3").topic.isEmpty should be(false)
    partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))
  }

  "TokenAssigner" should "not throw exception in case of less number of targets" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(2, "token-1", Some("target-1"), Seq("target-2").toArray),
      "token-2" -> TokenMetadata(2, "token-2", Some("target-2"), Seq("target-1").toArray)
    )
    val zookeeperPartitioningStore: MockZookeeperPartitioningStore = new MockZookeeperPartitioningStore(
      new Partitioning(initialPartitioning),
      curatorFramework,
      encoder
    )
    val tokenAssigner = new TokenAssigner(nodeId, zookeeperPartitioningStore)
    tokenAssigner.addToken("token-3", 2)

    val partitioning = zookeeperPartitioningStore.partitioning
    partitioning.partitioning.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3").sorted)
    partitioning.partitioning.values.flatten(_.primaryTarget).toSeq.sorted should be(Seq("target-1", "target-2", "target-4").sorted)
    partitioning.partitioning.values.map(_.topic).toSeq.sorted should be(Seq("token-1", "token-2", "token-3").sorted)

    partitioning.partitioning("token-3").topic.isEmpty should be(false)
    partitioning.partitioning("token-3").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-2"))

    partitioning.partitioning("token-1").topic.isEmpty should be(false)
    partitioning.partitioning("token-1").secondaryTargets.toSeq.sorted should be(Seq("target-2", "target-4"))

    partitioning.partitioning("token-2").topic.isEmpty should be(false)
    partitioning.partitioning("token-2").secondaryTargets.toSeq.sorted should be(Seq("target-1", "target-4"))
  }
}
