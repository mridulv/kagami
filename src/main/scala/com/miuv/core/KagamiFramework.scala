package com.miuv.core

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner._
import com.miuv.core.snapshot.{SnapshotMetadataEncoder, ZookeeperSnapshotMetadataStore}
import com.miuv.curator.{LeaderElection, NodeId}
import com.miuv.kafka.consumer.{ReplicatorClient, SimpleReplicatorReader, TokenNotFoundException}
import com.miuv.kafka.producer.SimpleReplicatorWriter
import com.miuv.util.ClientState
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryNTimes

import scala.collection.mutable.ListBuffer

class KagamiFramework(val connectionConfig: ConnectionConfig = ConnectionConfig()) {

  protected val nodeId = new NodeId

  private val curator = {
    val curatorFramework = CuratorFrameworkFactory.builder
      .connectString(connectionConfig.zookeeperConfig.zookeeperConnectionString)
      .namespace("kagami")
      .retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
      .connectionTimeoutMs(connectionConfig.zookeeperConfig.connectionTimeout)
      .build()
    curatorFramework.start()
    curatorFramework
  }

  private val zookeeperPartitioningStore = {
    val partitioningEncoder = new SimplePartitioningEncoder
    new ZookeeperPartitioningStore(curator, partitioningEncoder)
  }

  private val zookeeperSnapshotMetadaStore = {
    val snapShotMetadaEncoder: SnapshotMetadataEncoder = new SnapshotMetadataEncoder()
    new ZookeeperSnapshotMetadataStore(curator, snapShotMetadaEncoder)
  }

  private val leaderPartitioners: ListBuffer[LeaderPartitioner] = ListBuffer(new LeaderPartitioner(zookeeperPartitioningStore))

  private val leaderElection: LeaderElection = {
    new LeaderElection(
      curator,
      connectionConfig.zookeeperConfig.zookeeperConnectionString,
      nodeId = nodeId,
      leaderPartitioners)
  }

  protected val tokenAssigner: TokenAssigner = {
    new TokenAssigner(nodeId, zookeeperPartitioningStore)
  }

  private val simpleReplicatorReader: SimpleReplicatorReader = {
    new SimpleReplicatorReader(connectionConfig, zookeeperPartitioningStore, zookeeperSnapshotMetadaStore)
  }

  private val simplePartitioningListener: SimplePartitioningListener = {
    new SimplePartitioningListener(nodeId, simpleReplicatorReader)
  }

  zookeeperPartitioningStore.listen(simplePartitioningListener)

  def startConsumingRequests(replicatorClient: ReplicatorClient): Unit = {
    simpleReplicatorReader.setClientState(ClientState.Running, replicatorClient)
  }

  def startWriting(): SimpleReplicatorWriter = {
    new SimpleReplicatorWriter(tokenAssigner, nodeId, zookeeperPartitioningStore)
  }

}
