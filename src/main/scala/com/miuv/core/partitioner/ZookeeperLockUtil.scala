package com.miuv.core.partitioner

import java.util.concurrent.TimeUnit

import com.miuv.util.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.{InterProcessMutex, InterProcessSemaphoreMutex}

trait ZookeeperLockUtil extends Logging {

  val curatorFramework: CuratorFramework
  val lockPath: String
  lazy val lock = new InterProcessSemaphoreMutex(curatorFramework, lockPath)

  def withLock[T](fn: => T): T = {
    this.synchronized {
      lock.acquire(10000, TimeUnit.MILLISECONDS)
      try {
        fn
      } catch {
        case e: Exception => {
          error("Lock Acquire Failed with " + e.getMessage)
          throw e
        }
      } finally {
        lock.release()
      }
    }
  }

}
