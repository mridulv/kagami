package com.miuv.core.snapshot

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import com.miuv.core.Encoder
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.collection.JavaConverters._


class SnapshotMetadataEncoder extends Encoder[SnapshotMetadataInformation] {

  val schema = AvroSnapshotMetadataMapping.SCHEMA$

  override def serialize(snapshotMetadataInformation: SnapshotMetadataInformation = new SnapshotMetadataInformation()): Array[Byte] = {
    val snapshotMetadataMapping = snapshotMetadataInformation.metadata
    val keys = snapshotMetadataMapping.keys.map(_.asInstanceOf[CharSequence]).toList.asJava
    val metadatas = snapshotMetadataMapping.values.map(metadata => {
      val offset = metadata.consumerInfoOpt.map(consumerInfo => long2Long(consumerInfo.offset))
      val consumerGroup = metadata.consumerInfoOpt.map(_.consumerGroup)
      val partition = metadata.consumerInfoOpt.map(_.partition).map(int2Integer)
      val consumerInfo = AvroConsumerInfo.newBuilder()
        .setConsumer(consumerGroup.orNull)
        .setOffset(offset.orNull)
        .setPartition(partition.orNull)
        .build()
      AvroSnapshotMetadata.newBuilder()
        .setPath(metadata.path.orNull)
        .setConsumerInfo(consumerInfo)
        .build()
    }).toList
    val avroPartitioning = AvroSnapshotMetadataMapping.newBuilder()
      .setTokens(keys)
      .setSnapshotMetadata(metadatas.asJava)
      .build()

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val streamWriter = new DataFileWriter(new SpecificDatumWriter[AvroSnapshotMetadataMapping](schema)).create(schema, dos)
    streamWriter.append(avroPartitioning)
    streamWriter.flush()
    bos.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): SnapshotMetadataInformation = {
    val inputStream = new ByteArrayInputStream(bytes)
    val dataInputStream = new DataInputStream(inputStream)
    val datumReader = new SpecificDatumReader[AvroSnapshotMetadataMapping](schema)
    val streamReader = new DataFileStream(dataInputStream, datumReader)
    val avroSnapshotMetadataMapping = streamReader.next()

    val tokens = avroSnapshotMetadataMapping.getTokens.asScala.toList.map(_.toString)
    val snapshotMetadata = avroSnapshotMetadataMapping.getSnapshotMetadata.asScala.toList.map(snapshotMetadata => {
      val path = Option(snapshotMetadata.getPath).map(_.toString)
      val consumerInfo = Option(snapshotMetadata.getConsumerInfo).map(avroConsumerInfo => {
        val offset = avroConsumerInfo.getOffset
        val consumerGroup = avroConsumerInfo.getConsumer.toString
        val partition = avroConsumerInfo.getPartition
        ConsumerInfo(consumerGroup, partition, offset)
      })
      SnapshotMetadata(path, consumerInfo)
    })
    new SnapshotMetadataInformation(collection.mutable.Map(tokens.zip(snapshotMetadata): _*))
  }
}
