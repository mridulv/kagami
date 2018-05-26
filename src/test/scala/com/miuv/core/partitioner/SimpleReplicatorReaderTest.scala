package com.miuv.core.partitioner

import com.miuv.config.ConnectionConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.snapshot.{SnapshotMetadata, SnapshotMetadataInformation, ZookeeperSnapshotMetadataStore}
import com.miuv.curator.NodeId
import com.miuv.kafka.consumer.{ReplicatorClient, ReplicatorKafkaIntermediate, ReplicatorKafkaIntermediateFactory, SimpleReplicatorReader}
import com.miuv.util.ClientState
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.scalatest.concurrent._

import scala.collection.mutable

class SimpleReplicatorReaderTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach with Eventually {

  val nodeId = new NodeId()
  val connectionConfig = ConnectionConfig()
  val zookeeperPartitioningStore: ZookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
  val zookeeperSnapshotMetadaStore: ZookeeperSnapshotMetadataStore = mock[ZookeeperSnapshotMetadataStore]
  val simpleReplicatorReader: SimpleReplicatorReader = new SimpleReplicatorReader(zookeeperPartitioningStore, zookeeperSnapshotMetadaStore)

  "SimpleReplicatorReader" should "ensure replication of the tokens assigned to it" in {
    simpleReplicatorReader.mappedTokens.keys.headOption should be(None)
    simpleReplicatorReader.updateTokensForReplication(Seq("token-temp"))
    val replicatorClient: ReplicatorClient = mock[ReplicatorClient]
    val replicatorKafkaIntermediateFactory = mock[ReplicatorKafkaIntermediateFactory]
    val mockReplicatorKafkaIntermediate = mock[ReplicatorKafkaIntermediate]
    simpleReplicatorReader.setClientState(ClientState.Running, replicatorKafkaIntermediateFactory)

    val snapshotMetadataInformation = mutable.Map[Token, SnapshotMetadata](
      "token-1" -> SnapshotMetadata(Some("path-1"), 10, Some("target-2")),
      "token-2" -> SnapshotMetadata(Some("path-2"), 10, Some("target-1"))
    )

    val snapshotMetadata = SnapshotMetadata(Some("path-temp"), 100, Some("target-temp"))

    when(zookeeperSnapshotMetadaStore.load()).thenReturn(new SnapshotMetadataInformation(snapshotMetadataInformation))
    when(mockReplicatorKafkaIntermediate.takeSnapshot()).thenReturn(snapshotMetadata)
    when(replicatorKafkaIntermediateFactory.createReplicatorKafkaClient(any(), any())).thenReturn(mockReplicatorKafkaIntermediate)

    eventually {
      simpleReplicatorReader.mappedTokens.keys.toSeq.sorted should be(Seq("token-1", "token-2", "token-3"))

      simpleReplicatorReader.mappedTokens("token-1").snapshotMetadata should be(SnapshotMetadata(Some("path-1"), 10, Some("target-2")))
      simpleReplicatorReader.mappedTokens("token-2").snapshotMetadata should be(SnapshotMetadata(Some("path-2"), 10, Some("target-1")))
    }

    when(zookeeperSnapshotMetadaStore.store(any())).thenAnswer(new Answer[_]() {
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
