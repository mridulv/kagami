package com.miuv.kafka.consumer

import com.miuv.core.KagamiFramework
import com.miuv.core.partitioner.Partitioning.Token

trait Snapshotter {
  def takeSnapshot(token: Token): String
  def loadSnapshot(token: Token, path: String): Unit
}

// Revisit whether it should be a trait or an abstract class
abstract class KagamiClient(kagamiFramework: KagamiFramework) extends Snapshotter {

  type T
  kagamiFramework.startConsumingRequests(this)
  def replicateRequest(token: Token, request: T)
  def deserializeRequest(token: Token, request: Array[Byte]): T
}
