package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.curator.NodeId
import com.miuv.kafka.consumer.SimpleReplicatorReader

import scala.collection.mutable

class SimplePartitioningListener(nodeId: NodeId,
                                 simpleReplicatorReader: SimpleReplicatorReader)
  extends PartitioningListener {

  val snapShotForTokens: mutable.Set[Token] = mutable.Set[Token]().empty
  val tokensAlreadyPresent: mutable.Set[Token] = mutable.Set[Token]().empty

  override def notify(partitioning: Partitioning): Unit = {
    this.synchronized {
      val tokensMapped = partitioning.inversePartitioning().getOrElse(nodeId.nodeName, Seq[Token]())
      val tokensToBeReplicated = tokensMapped.filter(token => {
        !partitioning.partitioning(token).primaryTarget.contains(nodeId.nodeName)
      })
      val tokensYetToBeMapped = tokensToBeReplicated.filterNot(tokensAlreadyPresent.contains)
      simpleReplicatorReader.updateTokensForReplication(tokensYetToBeMapped)
      tokensYetToBeMapped.foreach(tokensAlreadyPresent.add)
      partitioning.partitioning.filter(_._2.snapshotReplica.contains(nodeId.nodeName)).keys.toSeq
    }
  }
}
