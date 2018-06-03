package com.miuv.core

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.Snapshotter

// Every ServiceInstance has to implement this Client for getting replication requests
abstract class KagamiClient(kagamiFramework: KagamiFramework) extends Snapshotter {

  type T
  kagamiFramework.startConsumingRequests(this)
  def replicateRequest(token: Token, request: T)
  def deserializeRequest(token: Token, request: Array[Byte]): T
}
