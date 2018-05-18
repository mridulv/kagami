package com.miuv.core.snapshot

import com.miuv.core.{Encoder, ZookeeperStore}
import com.miuv.core.partitioner.ZookeeperLockUtil
import com.miuv.curator.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.zookeeper.CreateMode

class ZookeeperSnapshotMetadataStore(override val curatorFramework: CuratorFramework,
                                     override val encoder: Encoder[SnapshotMetadataInformation])
  extends ZookeeperStore[SnapshotMetadataInformation]
    with ZookeeperLockUtil
    with Logging {

  override val path: String = "/snapshot/"
  override val lockPath: String = path
  override val defaultEntry: SnapshotMetadataInformation = new SnapshotMetadataInformation()
}
