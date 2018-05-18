package com.miuv.core.partitioner

trait ClientPartitioner[Token, Target] {
  def addToken(token: Token, numReplication: Int): Unit
}
