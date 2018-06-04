package com.miuv.core

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.Snapshotter

// Every ServiceInstance has to implement this Client for getting replication requests
trait KagamiClient extends Snapshotter {
  type T
  def replicateRequest(token: Token, request: T)
  def deserializeRequest(token: Token, request: Array[Byte]): T
}
