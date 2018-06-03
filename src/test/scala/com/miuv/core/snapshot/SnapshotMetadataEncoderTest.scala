package com.miuv.core.snapshot

import com.miuv.core.partitioner.Partitioning.Token
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SnapshotMetadataEncoderTest extends FlatSpec {
  val snapshotMetadataEncoder = new SnapshotMetadataEncoder
  val tokens = Seq("token1", "token2", "token3")
  val snapshotMetada = mutable.Map(
    tokens(0) -> SnapshotMetadata(Some("1"), Some(ConsumerInfo("con1", 0, 0L))),
    tokens(1) -> SnapshotMetadata(Some("2"), Some(ConsumerInfo("con2", 0, 0L))),
    tokens(2) -> SnapshotMetadata(Some("3"), Some(ConsumerInfo("con3", 0, 0L)))
  )
  val emptySnapshotMetadataInformation: mutable.Map[Token, SnapshotMetadata] = mutable.Map[Token, SnapshotMetadata]().empty

  "SnapshotMetadataEncoder" should "encode the values in proper way" in {
    val snapshotMetadataInformation = new SnapshotMetadataInformation(snapshotMetada)
    val finalSnapshotMetadataInformation = snapshotMetadataEncoder.deserialize(snapshotMetadataEncoder.serialize(snapshotMetadataInformation)).metadata
    assert(finalSnapshotMetadataInformation.keys.toSeq.sorted == tokens)
    assert(finalSnapshotMetadataInformation.values.flatten(_.consumerInfoOpt.map(_.offset)).toSet == Set(0))
    assert(finalSnapshotMetadataInformation.values.flatten(_.path).toSeq.sorted == Seq("1", "2", "3"))
  }

  "SnapshotMetadataEncoder" should "handle the empty maps gracefully" in {
    val snapshotMetadataInformation = new SnapshotMetadataInformation(emptySnapshotMetadataInformation)
    val finalSnapshotMetadataInformation = snapshotMetadataEncoder.deserialize(snapshotMetadataEncoder.serialize(snapshotMetadataInformation)).metadata
    assert(finalSnapshotMetadataInformation.keys.toSeq.sorted.isEmpty)
  }
}
