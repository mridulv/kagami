package com.miuv.kafka.consumer

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.partitioner.ZookeeperPartitioningStore
import com.miuv.core.ReplicatorReader
import com.miuv.core.snapshot.{SnapshotMetadata, ZookeeperSnapshotMetadataStore}
import com.miuv.util.{ClientState, StartStoppable}
import com.miuv.util.ClientState.ClientState

import scala.collection.mutable

class SimpleReplicatorReader(connectionConfig: ConnectionConfig,
                             val zookeeperPartitioningStore: ZookeeperPartitioningStore,
                             val zookeeperSnapshotMetadataStore: ZookeeperSnapshotMetadataStore)
  extends ReplicatorReader with StartStoppable {

  private var continue: Boolean = true
  private var clientState: ClientState = ClientState.NotStarted
  private var replicatorClient: ReplicatorClient = _

  init()

  // Need to handle the case where we have more than a single token on a serviceInstance
  val tokensToBeReplicated: mutable.Set[Token] = mutable.Set[Token]().empty
  val mappedTokens: mutable.Map[Token, ReplicatorKafkaIntermediate] = mutable.Map[Token, ReplicatorKafkaIntermediate]().empty

  private def execute1() = {
    val allowedTokens = this.synchronized {
      val tokens = tokensToBeReplicated.toSet -- mappedTokens.keys.toSet
      tokensToBeReplicated.clear()
      tokens
    }
    val tokensWithKafkaIntermediates = allowedTokens.zip(allowedTokens.map(token => {
      val replicatorKafkaIntermediate = startReplicationForToken(token, getSnapshotInformation(token))
      replicatorKafkaIntermediate.setup()
      replicatorKafkaIntermediate
    }))
    this.synchronized {
      tokensWithKafkaIntermediates.foreach(tokensWithKafkaIntermediate => {
        mappedTokens.put(tokensWithKafkaIntermediate._1, tokensWithKafkaIntermediate._2)
      })
    }
  }

  def getSnapshotInformation(token: Token): SnapshotMetadata = {
    val snapshotMetadataInformation = zookeeperSnapshotMetadataStore.load()
    snapshotMetadataInformation.metadata.getOrElse(token, SnapshotMetadata())
  }

  private def executeTask(): Unit = {
    mappedTokens.foreach(entry => {
      val snapshotMetadata = entry._2.takeSnapshot()
      zookeeperSnapshotMetadataStore.withLock({
        val metadata = zookeeperSnapshotMetadataStore.load()
        metadata.addTokenMetadata(entry._1, snapshotMetadata)
        zookeeperSnapshotMetadataStore.store(metadata)
      })
    })
  }

  private def init() = {
    start(execute1, 2 * 1000)
    start(executeTask, 2 * 60 * 60 * 1000)
  }

  override def doStart(): Unit = {
    continue = true
  }

  override protected def doStop(): Unit = {
    continue = false
  }

  private def start(fn: () => Unit, timePeriod: Long) = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while(continue) {
          if (clientState == ClientState.Running) {
            fn()
            Thread.sleep(timePeriod)
          }
        }
      }
    }).start()
  }

  def updateTokensForReplication(tokens: Seq[Token]): Unit = {
    this.synchronized {
      tokens.foreach(tokensToBeReplicated.add)
    }
  }

  private def startReplicationForToken(token: Token, snapshotMetadata: SnapshotMetadata) = {
    require(replicatorClient != null, "Replicator Has to be Initialized for the replication to start")
    new ReplicatorKafkaIntermediate(token, replicatorClient, snapshotMetadata, connectionConfig.kafkaConfig)
  }

  def setClientState(state: ClientState, replicatorClient: ReplicatorClient): Unit = {
    clientState = state
    this.replicatorClient = replicatorClient
  }
}

case class TokenNotFoundException(str: String) extends Exception