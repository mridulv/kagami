package com.miuv.kafka.consumer

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.partitioner.ZookeeperPartitioningStore
import com.miuv.core.snapshot.{ConsumerInfo, SnapshotMetadata, SnapshotMetadataInformation, ZookeeperSnapshotMetadataStore}
import com.miuv.curator.NodeId
import com.miuv.util.ClientState
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.{Matchers => NMatchers}
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.SpanSugar._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SimpleReplicatorReaderTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach with Eventually {

  val nodeId = new NodeId()
  val connectionConfig = ConnectionConfig()
  val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
  val zookeeperSnapshotMetadaStore: ZookeeperSnapshotMetadataStore = mock[ZookeeperSnapshotMetadataStore]
  val simpleReplicatorReader: SimpleReplicatorReader = new SimpleReplicatorReader(zookeeperPartitioningStore, zookeeperSnapshotMetadaStore)

  "SimpleReplicatorReader" should "ensure replication of the tokens assigned to it" in {

    val snapshotMetadataInformation = mutable.Map[Token, SnapshotMetadata](
      "token-1" -> SnapshotMetadata(Some("path-1"), Some(ConsumerInfo("target-2", 0, 0L))),
      "token-2" -> SnapshotMetadata(Some("path-2"), Some(ConsumerInfo("target-1", 0, 0L)))
    )

    val snapshotMetadata = SnapshotMetadata(Some("path-temp"), Some(ConsumerInfo("target-temp", 0, 0L)))

    val replicatorClient: KagamiClient = mock[KagamiClient]
    val replicatorKafkaIntermediateFactory = mock[ReplicatorKafkaIntermediateFactory]
    val mockReplicatorKafkaIntermediate = mock[KagamiClientIntermediate]

    when(zookeeperSnapshotMetadaStore.load()).thenReturn(new SnapshotMetadataInformation(snapshotMetadataInformation))
    when(replicatorKafkaIntermediateFactory.createReplicatorKafkaClient(NMatchers.eq("token-temp"), any())).thenReturn(mockReplicatorKafkaIntermediate)
    when(mockReplicatorKafkaIntermediate.takeSnapshot()).thenReturn(snapshotMetadata)
    when(mockReplicatorKafkaIntermediate.snapshotMetadata).thenReturn(SnapshotMetadata(Some("path-temp"), Some(ConsumerInfo("target-2", 0, 0L))))

    simpleReplicatorReader.mappedTokens.keys.headOption should be(None)
    simpleReplicatorReader.updateTokensForReplication(Seq("token-temp"))
    simpleReplicatorReader.setClientState(ClientState.Running, replicatorKafkaIntermediateFactory)

    eventually(timeout(1.minute)) {
      simpleReplicatorReader.mappedTokens.keys.toSeq.sorted should be(Seq("token-temp"))

      simpleReplicatorReader.mappedTokens("token-temp").snapshotMetadata should be(SnapshotMetadata(Some("path-temp"), Some(ConsumerInfo("target-2", 0, 0L))))
    }

    when(zookeeperSnapshotMetadaStore.store(any())).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val snapshotMetadaInformation = args(0).asInstanceOf[SnapshotMetadataInformation]
        snapshotMetadaInformation.metadata.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-temp"))
        snapshotMetadaInformation.metadata.values.flatten(_.path).toSeq.sorted should be(Seq("path-1", "path-2", "path-temp"))
      }
    })
  }
}
