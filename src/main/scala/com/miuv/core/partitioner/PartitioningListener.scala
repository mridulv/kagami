package com.miuv.core.partitioner

trait PartitioningListener {
  def notify(partitioning: Partitioning)
}
