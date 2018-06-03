package com.miuv.core.partitioner

trait TestLockUtil extends ZookeeperLockUtil {
  val obj = new Object
  override def withLock[T](fn: => T): T = {
    obj.synchronized {
      fn
    }
  }
}
