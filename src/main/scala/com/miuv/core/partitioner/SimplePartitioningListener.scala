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

  override def notifyListener(partitioning: Partitioning): Unit = {
    this.synchronized {
      val tokensMapped = partitioning.inversePartitioning().getOrElse(nodeId.nodeName, Seq[Token]())
      // Note(mridul, 2018-05-26): In this there is an implicit assumption which is wrong though that inverse
      // partitioning would never contain or the inverse partitioning would never contain two same tokens on a
      // single node. So either a token can be primary in which case it would be registered by the writer or either
      // it has to be replicated which in turn has to be managed by us.
      val tokensToBeReplicated = tokensMapped.filter(token => {
        !partitioning.partitioning(token).primaryTarget.contains(nodeId.nodeName)
      })
      val tokensYetToBeMapped = tokensToBeReplicated.filterNot(tokensAlreadyPresent.contains)
      simpleReplicatorReader.updateTokensForReplication(tokensYetToBeMapped)
      tokensYetToBeMapped.foreach(tokensAlreadyPresent.add)
    }
  }
}
