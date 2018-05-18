package com.miuv.core.snapshot

import com.miuv.core.Encoder


class SnapshotMetadataEncoder extends Encoder[SnapshotMetadataInformation] {

  val sep1 = "::"
  val sep2 = ":"
  val metadataSep = "--"
  val NoneIdentifier = "None"

  override def serialize(snapshotMetadataInformation: SnapshotMetadataInformation = new SnapshotMetadataInformation()): Array[Byte] = {
    val metadata = snapshotMetadataInformation.metadata
    val keys = metadata.keys.mkString(sep2)
    val values = metadata.values.map(tokenMetadata => {
      tokenMetadata.path.map(_.toString).getOrElse("None") +
        metadataSep + tokenMetadata.offset +
        metadataSep + tokenMetadata.consumer.map(_.toString).getOrElse("None")
    }).mkString(sep2)
    (keys + sep1 + values).map(_.toByte).toArray
  }

  private def parseOption(opt: String) = {
    if (opt == "None") {
      None
    } else {
      Some(opt)
    }
  }

  override def deserialize(bytes: Array[Byte]): SnapshotMetadataInformation = {
    val partitioningData = bytes.map(_.toChar).toString
    val keys = partitioningData.split(sep1)(0).split(sep2)
    val values = partitioningData.split(sep1)(1)

    val metadatas = values.split(sep2).map(metadata => {
      val path = parseOption(metadata.split(metadataSep)(0))
      val offset = metadata.split(metadataSep)(1).toInt
      val consumer = parseOption(metadata.split(metadataSep)(2))
      SnapshotMetadata(path, offset, consumer)
    })

    val snapshotMetadataInformation = keys.zip(metadatas).toMap
    new SnapshotMetadataInformation(collection.mutable.Map(snapshotMetadataInformation.toSeq: _*))
  }
}
