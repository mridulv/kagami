package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning._

import scala.collection.mutable

class Partitioning(val partitioning: mutable.Map[Token, TokenMetadata] = mutable.Map[Token, TokenMetadata]().empty) {

  def addToken(token: Token, tokenMetadata: TokenMetadata): Unit = {
    partitioning.get(token) match {
      case Some(metadata) => {
        val data = TokenMetadata(tokenMetadata.replication, tokenMetadata.topic, tokenMetadata.primaryTarget, metadata.secondaryTargets)
        partitioning.put(token, data)
      }
      case None => partitioning.put(token, tokenMetadata)
    }
  }

  override def toString: String = {
    val keys = partitioning.keys
    val values = partitioning.values
    keys.mkString(",") + "------" + values.mkString(",")
  }

  def removeTargetFromPartitioning(target: Target): Unit = {
    partitioning.map(entry => {
      val token = entry._1
      val metadata = entry._2
      val primaryTarget = if (metadata.primaryTarget.contains(target)) {
        None
      } else {
        metadata.primaryTarget
      }
      val secondaryTargets = metadata.secondaryTargets.toSet
      val replicaTargets = (secondaryTargets - target).toArray
      val topic = metadata.topic
      val updatedMetadata = TokenMetadata(metadata.replication, topic, primaryTarget, replicaTargets)
      partitioning.put(token, updatedMetadata)
    })
  }

  def inversePartitioning(): Map[Target, Seq[Token]] = {
    partitioning.flatten(tokenInfo => {
      val token = tokenInfo._1
      (tokenInfo._2.secondaryTargets ++ tokenInfo._2.primaryTarget.map(Array(_)).getOrElse(Array())).map(target => (target, token))
    }).groupBy(_._1).map(elem => (elem._1, elem._2.map(_._2).toSeq))
  }

  def addTargetToToken(token: Token, target: Target): Unit = {
    require(partitioning.get(token).isDefined)
    val metadata = partitioning(token)
    val numReplication = metadata.replication
    val topic = metadata.topic
    val redefinedSecondaryTargets: Array[String] = metadata.secondaryTargets ++ Array(target)
    partitioning.put(token, TokenMetadata(numReplication, topic, metadata.primaryTarget, redefinedSecondaryTargets))
  }
}

case class TokenMetadata(replication: Int, topic: String, primaryTarget: Option[Target], secondaryTargets: Array[Target]) {
  override def toString: Token = {
    s" [ Replication is: $replication and primaryTarget is: $primaryTarget and topicInfo: $topic and secondaryTargets ${secondaryTargets.mkString(",")} ] "
  }
}

object Partitioning {
  type Token = String
  type Target = String
}