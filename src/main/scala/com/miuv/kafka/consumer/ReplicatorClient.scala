package com.miuv.kafka.consumer

import com.miuv.core.KagamiFramework
import com.miuv.core.partitioner.Partitioning.Token

trait Snapshotter {
  def takeSnapshot(token: Token): String
  def loadSnapshot(token: Token, path: String): Unit
}

trait ReplicatorClient extends Snapshotter {

  val kagamiFramework: KagamiFramework
  kagamiFramework.startConsumingRequests(this)
  def makeRequest[T](token: Token, request: T)
  def deserializeRequest[T](token: Token, request: Array[Byte]): T
}
