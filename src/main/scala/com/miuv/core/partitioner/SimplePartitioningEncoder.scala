package com.miuv.core.partitioner

import com.miuv.core.Encoder

class SimplePartitioningEncoder extends Encoder[Partitioning] {

  val sep1 = "::"
  val sep2 = ":"
  val metadataSep = "--"
  val targetsSep = ";"

  override def serialize(partitioning: Partitioning): Array[Byte] = {
    val partitioningData = partitioning.partitioning
    val keys = partitioningData.keys.mkString(sep2)
    val values = partitioningData.values.map(tokenMetadata => {
      tokenMetadata.replication + metadataSep + tokenMetadata.secondaryTargets.mkString(targetsSep)
    }).mkString(sep2)
    (keys + sep1 + values).map(_.toByte).toArray
  }

  override def deserialize(bytes: Array[Byte]): Partitioning = {
    val partitioningData = bytes.map(_.toChar).toString
    val keys = partitioningData.split(sep1)(0).split(sep2)
    val values = partitioningData.split(sep1)(1)

    val metadatas = values.split(sep2).map(metadata => {
      val numReplication = metadata.split(metadataSep)(0).toInt
      val targets = metadata.split(metadataSep)(1).split(targetsSep)
      // Need to handle this generically
      TokenMetadata(numReplication, None, None, targets)
    })

    val partitioning = keys.zip(metadatas).toMap
    new Partitioning(collection.mutable.Map(partitioning.toSeq: _*))
  }
}
