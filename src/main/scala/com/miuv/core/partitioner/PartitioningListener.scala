package com.miuv.core.partitioner

trait PartitioningListener {
  def notifyListener(partitioning: Partitioning)
}
