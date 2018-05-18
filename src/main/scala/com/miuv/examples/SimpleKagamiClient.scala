package com.miuv.examples

import com.miuv.core.KagamiFramework
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.kafka.consumer.ReplicatorClient
import com.miuv.kafka.producer.SimpleReplicatorWriter

class SimpleKagamiClient extends ReplicatorClient {

  val kagamiFramework: KagamiFramework = new KagamiFramework()
  val kagamiWriter: SimpleReplicatorWriter = kagamiFramework.startWriting()

  override def makeRequest[T](token: Token, request: T): Unit = {
  }

  override def deserializeRequest[T](token: Token, request: Array[Byte]): T = {
  }

  override def takeSnapshot(token: Token): String = {
  }

  override def loadSnapshot(token: Token, path: String): Unit = {

  }
}
