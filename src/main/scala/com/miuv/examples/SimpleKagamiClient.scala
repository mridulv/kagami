package com.miuv.examples

import com.miuv.core.{KagamiClient, KagamiFramework}
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.kafka.producer.{KagamiProducerIntermediate, SimpleReplicatorWriter}
import com.miuv.util.{Logging, StringUtils}

import scala.collection.mutable

class SimpleKagamiClient(kagamiFramework: KagamiFramework, clientToken: Token) extends KagamiClient with Logging {

  val kagamiWriter: SimpleReplicatorWriter = kagamiFramework.init(this)
  val kafkaWriterIntermediate: KagamiProducerIntermediate = kagamiWriter.add(clientToken, 1)

  new Thread(new Runnable {
    override def run(): Unit = {
      while (true) {
        val str = StringUtils.randomString(5)
        val content = str.map(_.toByte).toArray
        receiverReplicatedData(clientToken, serialize(str))
        kafkaWriterIntermediate.sendDataForReplication(content)
        Thread.sleep(5000)
      }
    }
  }).start()

  var map: mutable.Map[Token, mutable.Map[String, Int]] = mutable.Map[Token, mutable.Map[String, Int]]()

  override def receiverReplicatedData(token: Token, data: Array[Byte]): Unit = {
    val request = deserialize(data)
    map.synchronized {
      map.get(token) match {
        case Some(tokenMap) => {
          val tokenMapValue = tokenMap.getOrElse(request, 0)
          tokenMap.put(request, tokenMapValue + 1)
        }
        case None => {
          map.put(token, mutable.Map[String, Int]())
          val tokenMap = map(token)
          val tokenMapValue = tokenMap.getOrElse(request, 0)
          tokenMap.put(request, tokenMapValue + 1)
        }
      }
    }
  }

  def deserialize(bytes: Array[Byte]): String = {
    new String(bytes)
  }

  def serialize(data: String): Array[Byte] = {
    data.toCharArray.map(_.toByte)
  }

  override def takeSnapshot(token: Token): String = {
    info("Saving the SNAPSHOT")
    map.get(token) match {
      case None => {
        val value = mutable.Map[String, Int]()
        map.put(token, value)
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
