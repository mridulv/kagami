package com.miuv.examples

import com.miuv.core.{KagamiClient, KagamiFramework}
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.kafka.producer.{KagamiProducerIntermediate, SimpleReplicatorWriter}
import com.miuv.util.{Logging, StringUtils}

import scala.collection.mutable

class SimpleKagamiClient(kagamiFramework: KagamiFramework, clientToken: Token) extends KagamiClient(kagamiFramework) with Logging {

  type T = String

  val kagamiWriter: SimpleReplicatorWriter = kagamiFramework.startWriting()
  val kafkaWriterIntermediate: KagamiProducerIntermediate = kagamiWriter.add(clientToken, 1)

  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        val str = StringUtils.randomString(5)
        val content = str.map(_.toByte).toArray
        replicateRequest(clientToken, str)
        kafkaWriterIntermediate.replicateEntry(content)
        Thread.sleep(2000)
      }
    }
  }).start()

  var map: mutable.Map[Token, mutable.Map[String, Int]] = mutable.Map[Token, mutable.Map[String, Int]]()

  override def replicateRequest(token: Token, request: String): Unit = {
    map.synchronized {
      info(s"Going to add the entry for $token: the content $request")
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
