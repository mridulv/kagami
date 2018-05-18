package com.miuv.service_registry

import com.miuv.curator.LeaderElection.basePath
import org.apache.curator.framework.CuratorFramework
import scala.collection.JavaConverters._

class ServiceRegistry(curatorFramework: CuratorFramework) {

  def load(): Seq[String] = {
    val children = curatorFramework.getChildren.forPath(basePath)
    children.asScala
  }

}
