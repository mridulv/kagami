package com.miuv.core.partitioner

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import com.miuv.core.Encoder
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}

import scala.collection.JavaConverters._

class SimplePartitioningEncoder extends Encoder[Partitioning] {

  val schema = AvroPartitioning.SCHEMA$

  override def serialize(partitioning: Partitioning): Array[Byte] = {
    val partitioningData = partitioning.partitioning
    val keys = partitioningData.keys.map(_.asInstanceOf[CharSequence]).toList.asJava
    val metadatas = partitioningData.values.map(metadata => {
      val secondaryTargets = metadata.secondaryTargets.map(_.asInstanceOf[CharSequence]).toList.asJava
      AvroTokenMetadata.newBuilder()
        .setPrimarytarget(metadata.primaryTarget.orNull)
        .setTopic(metadata.topic)
        .setReplication(metadata.replication)
        .setSecondaryTargets(secondaryTargets)
        .build()
    }).toList
    val avroPartitioning = AvroPartitioning.newBuilder()
      .setTokens(keys)
      .setTokenMetadatas(metadatas.asJava)
      .build()

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    val streamWriter = new DataFileWriter(new SpecificDatumWriter[AvroPartitioning](schema)).create(schema, dos)
    streamWriter.append(avroPartitioning)
    streamWriter.flush()
    bos.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): Partitioning = {
    val inputStream = new ByteArrayInputStream(bytes)
    val dataInputStream = new DataInputStream(inputStream)
    val datumReader = new SpecificDatumReader[AvroPartitioning](schema)
    val streamReader = new DataFileStream(dataInputStream, datumReader)
    val avroPartitioning = streamReader.next()

    val tokens = avroPartitioning.getTokens.asScala.toList.map(_.toString)
    val tokenMetadatas = avroPartitioning.getTokenMetadatas.asScala.toList.map(tokenMetadata => {
      val replication = tokenMetadata.getReplication
      val topic = tokenMetadata.getTopic
      val primaryTarget = tokenMetadata.getPrimarytarget
      val secondaryTargets = tokenMetadata.getSecondaryTargets

      TokenMetadata(
        replication,
        topic.toString,
        Option(primaryTarget).map(_.toString),
        secondaryTargets.asScala.toArray.map(_.toString)
      )
    })
    new Partitioning(collection.mutable.Map(tokens.zip(tokenMetadatas): _*))
  }
}
