package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SimplePartitioningEncoderTest extends FlatSpec {

  val simplePartitioningEncoder = new SimplePartitioningEncoder
  val targets = Seq("node-1", "node-2", "node-3")
  val tokens = Seq("token1", "token2", "token3")
  val partitioningData = mutable.Map(
    tokens(0) -> TokenMetadata(1, "topic-1", Some(targets(0)), Array(targets(1), targets(2))),
    tokens(1) -> TokenMetadata(1, "topic-2", Some(targets(1)), Array(targets(2), targets(0))),
    tokens(2) -> TokenMetadata(1, "topic-3", Some(targets(2)), Array(targets(0), targets(1)))
  )
  val emptyPartitioningData: mutable.Map[Token, TokenMetadata] = mutable.Map[Token, TokenMetadata]().empty

  "SimplePartitioningEncoder" should "encode the values in proper way" in {
    val partitioning = new Partitioning(partitioningData)
    val resultantPartitioning = simplePartitioningEncoder.deserialize(simplePartitioningEncoder.serialize(partitioning))
    assert(resultantPartitioning.partitioning.keys.toSeq.sorted == tokens)
    assert(resultantPartitioning.partitioning.values.toSeq.flatten(_.primaryTarget).toSeq.sorted == List("node-1", "node-2", "node-3"))
    assert(resultantPartitioning.partitioning.values.toSeq.map(_.topic).toSeq.sorted == List("topic-1", "topic-2", "topic-3"))
  }

  "SimplePartitioningEncoder" should "handle the empty maps gracefully" in {
    val partitioning = new Partitioning(emptyPartitioningData)
    val resultantPartitioning = simplePartitioningEncoder.deserialize(simplePartitioningEncoder.serialize(partitioning))
    assert(resultantPartitioning.partitioning.keys.isEmpty)
  }

  "SimplePartitioningEncoder" should "handle the cases when primary target is empty and secondary targets are empty" in {
    val partitioningData = mutable.Map(
      tokens(0) -> TokenMetadata(1, "topic1", None, Array()),
      tokens(1) -> TokenMetadata(1, "topic1", None, Array()),
      tokens(2) -> TokenMetadata(1, "topic1", None, Array(targets(0), targets(1)))
    )
    val partitioning = new Partitioning(partitioningData)
    val resultantPartitioning = simplePartitioningEncoder.deserialize(simplePartitioningEncoder.serialize(partitioning))
    assert(resultantPartitioning.partitioning.keys.toSeq.sorted == tokens)
    assert(resultantPartitioning.partitioning.values.toSeq.flatten(_.primaryTarget).isEmpty)
    assert(resultantPartitioning.partitioning.values.toSeq.flatMap(_.secondaryTargets).size == 2)
  }
}

