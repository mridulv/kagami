package com.miuv.core.snapshot

import com.miuv.core.partitioner.Partitioning.Token

trait Snapshotter {
  def takeSnapshot(token: Token): String
  def loadSnapshot(token: Token, path: String): Unit
}
