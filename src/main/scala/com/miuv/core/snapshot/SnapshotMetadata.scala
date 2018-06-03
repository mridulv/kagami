package com.miuv.core.snapshot

import com.miuv.core.partitioner.Partitioning.Token

import scala.collection.mutable

case class ConsumerInfo(consumerGroup: String, partition: Int, offset: Long) {
  override def toString: String = {
    s" ConsumerGroup ${consumerGroup} and partition ${partition} and offset ${offset} "
  }
}

case class SnapshotMetadata(path: Option[String] = None, consumerInfoOpt: Option[ConsumerInfo] = None) {
  override def toString: String = {
    s" path: ${path} and consumerInfo [${consumerInfoOpt}] "
  }
}

class SnapshotMetadataInformation(val metadata: mutable.Map[Token, SnapshotMetadata] = mutable.Map[Token, SnapshotMetadata]()) {

  def addTokenMetadata(token: Token, snapshotMetadata: SnapshotMetadata): Unit = {
    metadata.put(token, snapshotMetadata)
  }

  override def toString: String = {
    val keys = metadata.keys.mkString(",")
    val values = metadata.values.mkString(",,,")
    keys + "----" + values + "\n"
  }

}