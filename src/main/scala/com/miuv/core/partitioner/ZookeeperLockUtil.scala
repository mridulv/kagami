package com.miuv.core.partitioner

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessMutex

trait ZookeeperLockUtil {

  val curatorFramework: CuratorFramework
  val lockPath: String
  lazy val lock = new InterProcessMutex(curatorFramework, lockPath)

  def withLock[T](fn: => T): T = {
    lock.acquire()
    val ret = fn
    lock.release()
    ret
  }

}
