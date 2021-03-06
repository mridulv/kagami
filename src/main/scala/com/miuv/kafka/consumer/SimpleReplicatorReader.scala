package com.miuv.kafka.consumer

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.partitioner.ZookeeperPartitioningStore
import com.miuv.core.ReplicatorReader
import com.miuv.core.snapshot.{SnapshotMetadata, ZookeeperSnapshotMetadataStore}
import com.miuv.util.{ClientState, Logging, StartStoppable}
import com.miuv.util.ClientState.ClientState

import scala.collection.mutable

class SimpleReplicatorReader(val zookeeperPartitioningStore: ZookeeperPartitioningStore,
                             val zookeeperSnapshotMetadataStore: ZookeeperSnapshotMetadataStore)
  extends ReplicatorReader with StartStoppable with Logging {

  private var continue: Boolean = true
  private var clientState: ClientState = ClientState.NotStarted
  private var replicatorKafkaIntermediateFactory: ReplicatorKafkaIntermediateFactory = _

  init()

  // Note(mridul, 2018-05-18): Need to handle the case where we have more than a single token on a serviceInstance
  // Note(mridul, 2018-05-26): Change these vals to private and ensure they are visible to tests.
  val tokensToBeReplicated: mutable.Set[Token] = mutable.Set[Token]().empty
  val mappedTokens: mutable.Map[Token, KagamiClientIntermediate] = mutable.Map[Token, KagamiClientIntermediate]().empty

  private def tokensReplicationTask() = {
    val allowedTokens = this.synchronized {
      val tokens = tokensToBeReplicated.toSet -- mappedTokens.keys.toSet
      tokensToBeReplicated.clear()
      tokens
    }
    val tokensWithKafkaIntermediates = allowedTokens.zip(allowedTokens.map(token => {
      val kagamiClientIntermediate = startReplicationForToken(token, getSnapshotInformation(token))
      kagamiClientIntermediate.setup()
      kagamiClientIntermediate
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

  private def snapshotTask(): Unit = {
    mappedTokens.foreach(entry => {
      snapshotForToken(entry._1, entry._2)
    })
  }

  private def snapshotForToken(token: Token, kagamiClientIntermediate: KagamiClientIntermediate) = {
    val snapshotMetadata = kagamiClientIntermediate.takeSnapshot()
    zookeeperSnapshotMetadataStore.withLock({
      val metadata = zookeeperSnapshotMetadataStore.load()
      metadata.addTokenMetadata(token, snapshotMetadata)
      zookeeperSnapshotMetadataStore.store(metadata)
    })
  }

  private def init() = {
    // Make this time controllable via UTs as well , so that they dont have to wait for a long time
    start(tokensReplicationTask, 2 * 1000)
    start(snapshotTask, 30 * 1000)
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
          }
          //info(s"We are sleeping here for ${timePeriod}")
          Thread.sleep(timePeriod)
        }
      }
    }).start()
  }

  def updateTokensForReplication(tokens: Seq[Token]): Unit = {
    info(s"We are getting following tokens: ${tokens.mkString(".")}")
    this.synchronized {
      tokens.foreach(tokensToBeReplicated.add)
    }
  }

  private def startReplicationForToken(token: Token, snapshotMetadata: SnapshotMetadata): KagamiClientIntermediate = {
    replicatorKafkaIntermediateFactory.createReplicatorKafkaClient(token, snapshotMetadata)
  }

  def setClientState(state: ClientState, replicatorKafkaIntermediateFactory: ReplicatorKafkaIntermediateFactory): Unit = {
    clientState = state
    this.replicatorKafkaIntermediateFactory = replicatorKafkaIntermediateFactory
  }
}

case class TokenNotFoundException(str: String) extends Exception