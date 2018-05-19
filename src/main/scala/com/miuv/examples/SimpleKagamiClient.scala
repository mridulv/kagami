package com.miuv.examples

import com.miuv.core.KagamiFramework
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.kafka.consumer.ReplicatorClient
import com.miuv.kafka.producer.{KafkaWriterIntermediate, SimpleReplicatorWriter}
import com.miuv.util.Logging

import scala.collection.mutable
import scala.util.Random

class SimpleKagamiClient(kagamiFramework: KagamiFramework) extends ReplicatorClient(kagamiFramework) with Logging {

  type T = String

  val kagamiWriter: SimpleReplicatorWriter = kagamiFramework.startWriting()
  val kafkaWriterIntermediate: KafkaWriterIntermediate = kagamiWriter.add("token1", 1)

  new Thread(new Runnable {
    override def run(): Unit = {
      val str = Random.alphanumeric(2).toString
      val content = str.map(_.toByte).toArray
      makeRequest("token1", str)
      info(s"Would replicate the token1 for the content ${str}")
      kafkaWriterIntermediate.replicateEntry(content)
    }
  }).start()

  var map: mutable.Map[Token, mutable.Map[String, Int]] = mutable.Map[Token, mutable.Map[String, Int]]()

  override def makeRequest(token: Token, request: String): Unit = {
    map.synchronized {
      info(s"We are going to replicate the token1 for the content ${request}")
      map.put(token, mutable.Map[String, Int]())
      val tokenMap = map(token)
      val tokenMapValue = tokenMap.getOrElse(request, 0)
      tokenMap.put(request, tokenMapValue + 1)
    }
  }

  override def deserializeRequest(token: Token, request: Array[Byte]): String = {
    new String(request)
  }

  override def takeSnapshot(token: Token): String = {
    info("Saving the SNAPSHOT")
    map.get(token) match {
      case None => {
        val value = mutable.Map[String, Int]()
        val emptyMap = map.put(token, value)
        SerDeserSnapshot.addEntry(value)
      }
      case Some(snapshot) => {
        SerDeserSnapshot.addEntry(snapshot)
      }
    }
  }

  override def loadSnapshot(token: Token, path: String): Unit = {
    info("Loading the SNAPSHOT")
    map.put(token, SerDeserSnapshot.getEntry(path))
  }
}
