package com.miuv.examples

import com.miuv.core.KagamiClient
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.util.Logging

import scala.collection.mutable

class SimpleKagamiClient extends KagamiClient with Logging {

  var map: mutable.Map[Token, mutable.Map[String, Int]] = mutable.Map[Token, mutable.Map[String, Int]]()

  def addEntryToMap(token: Token, request: String): Unit = {
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

  override def receiveReplicatedData(token: Token, data: Array[Byte]): Unit = {
    val request = deserialize(data)
    addEntryToMap(token, request)
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
