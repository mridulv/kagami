package com.miuv.core.snapshot

import com.miuv.core.partitioner.Partitioning.Token

trait Snapshotter {
  type Path = String
  def takeSnapshot(token: Token): Path
  def loadSnapshot(token: Token, path: Path): Unit
}
