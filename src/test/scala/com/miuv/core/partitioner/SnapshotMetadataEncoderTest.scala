package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.{SnapshotMetadata, SnapshotMetadataEncoder, SnapshotMetadataInformation}

import scala.collection.mutable
import org.scalatest._

class SnapshotMetadataEncoderTest extends FlatSpec {
  val snapshotMetadataEncoder = new SnapshotMetadataEncoder
  val tokens = Seq("token1", "token2", "token3")
  val snapshotMetada = mutable.Map(
    tokens(0) -> SnapshotMetadata(Some("1"), 1, Some("con1")),
    tokens(1) -> SnapshotMetadata(Some("2"), 1, Some("con2")),
    tokens(2) -> SnapshotMetadata(Some("3"), 1, Some("con3"))
  )
  val emptySnapshotMetadataInformation: mutable.Map[Token, SnapshotMetadata] = mutable.Map[Token, SnapshotMetadata]().empty

  "SnapshotMetadataEncoder" should "encode the values in proper way" in {
    val snapshotMetadataInformation = new SnapshotMetadataInformation(snapshotMetada)
    val finalSnapshotMetadataInformation = snapshotMetadataEncoder.deserialize(snapshotMetadataEncoder.serialize(snapshotMetadataInformation)).metadata
    assert(finalSnapshotMetadataInformation.keys.toSeq.sorted == tokens)
    assert(finalSnapshotMetadataInformation.values.map(_.offset).toSet == Set(1))
    assert(finalSnapshotMetadataInformation.values.flatten(_.path).toSeq.sorted == Seq("1", "2", "3"))
  }

  "SnapshotMetadataEncoder" should "handle the empty maps gracefully" in {
    val snapshotMetadataInformation = new SnapshotMetadataInformation(emptySnapshotMetadataInformation)
    val finalSnapshotMetadataInformation = snapshotMetadataEncoder.deserialize(snapshotMetadataEncoder.serialize(snapshotMetadataInformation)).metadata
    assert(finalSnapshotMetadataInformation.keys.toSeq.sorted.isEmpty)
  }
}
