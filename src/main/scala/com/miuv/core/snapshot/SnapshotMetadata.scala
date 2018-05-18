package com.miuv.core.snapshot

import com.miuv.core.partitioner.Partitioning.Token

import scala.collection.mutable

case class SnapshotMetadata(path: Option[String] = None, offset: Long = -1, consumer: Option[String] = None)

class SnapshotMetadataInformation(val metadata: mutable.Map[Token, SnapshotMetadata] = mutable.Map[Token, SnapshotMetadata]()) {

  def addTokenMetadata(token: Token, snapshotMetadata: SnapshotMetadata): Unit = {
    metadata.put(token, snapshotMetadata)
  }

}