package com.miuv.kafka.producer

import com.miuv.kafka.KafkaProducerConfig
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.core.partitioner.{TokenAssigner, ZookeeperPartitioningStore}
import com.miuv.core.ReplicatorWriter
import com.miuv.curator.NodeId

import scala.util.Random

class SimpleReplicatorWriter(tokenAssigner: TokenAssigner,
                             nodeId: NodeId,
                             override protected val zookeeperPartitioningStore: ZookeeperPartitioningStore)
  extends ReplicatorWriter {

  override def add(token: Token, numReplication: Int = 2): KagamiProducerIntermediate = {
    // Note: Improve this method , we cannot expose the KafkaWriterIntermediate to the outer world
    tokenAssigner.addToken(token, numReplication)
    new KagamiProducerIntermediate(createKafkaProducer(token))
  }

  private def createKafkaProducer(token: Token): KagamiKafkaProducer = {
    val kafkaProducerConfig = new KafkaProducerConfig(serverId = nodeId.nodeName, topic = token, kafkaConnectString = "localhost:9092")
    new KagamiKafkaProducer(kafkaProducerConfig)
  }
}
