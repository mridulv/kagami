package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import com.miuv.kafka.consumer.SimpleReplicatorReader
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.mutable

class SimplePartitionerListenerTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {
  val nodeId = new NodeId("target-1")
  val simpleReplicatorReader: SimpleReplicatorReader = mock[SimpleReplicatorReader]
  val simplePartitionerListener: SimplePartitioningListener = new SimplePartitioningListener(nodeId, simpleReplicatorReader)

  "SimplePartitionerListener" should "listen to the partition changes and make necessary changes" in {
    val initialPartitioning: mutable.Map[Token, TokenMetadata] = mutable.Map(
      "token-1" -> TokenMetadata(2, Some("target-1"), Some("target-2"), Seq("target-1").toArray),
      "token-2" -> TokenMetadata(2, Some("target-2"), Some("target-3"), Seq("target-4", "target-1").toArray),
      "token-3" -> TokenMetadata(2, Some("target-3"), Some("target-4"), Seq("target-2", "target-1").toArray)
    )
    val partitioning = new Partitioning(initialPartitioning)
    simplePartitionerListener.notifyListener(partitioning)

    when(simpleReplicatorReader.updateTokensForReplication(any())).thenAnswer(new Answer[_]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val tokens = args(0).asInstanceOf[Seq[Token]]
        tokens should be(Seq("token-1", "token-2", "token-3"))
      }
    })
  }
}
