package com.miuv.core.partitioner

import com.miuv.core.partitioner.Partitioning._

import scala.collection.mutable

class Partitioning(val partitioning: mutable.Map[Token, TokenMetadata] = mutable.Map[Token, TokenMetadata]().empty) {

  def addToken(token: Token, tokenMetadata: TokenMetadata): Unit = {
    partitioning.get(token) match {
      case Some(metadata) => {
        val data = TokenMetadata(tokenMetadata.replication, tokenMetadata.primaryTarget, metadata.secondaryTargets.headOption, metadata.secondaryTargets)
        partitioning.put(token, data)
      }
      case None => partitioning.put(token, tokenMetadata)
    }
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
      val snapshotReplica = if (metadata.snapshotReplica.contains(target) || metadata.snapshotReplica.isEmpty) {
        replicaTargets.headOption
      } else {
        metadata.snapshotReplica
      }
      val updatedMetadata = TokenMetadata(metadata.replication, primaryTarget, snapshotReplica, replicaTargets)
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
    val redefinedSecondaryTargets: Array[String] = metadata.secondaryTargets ++ Array(target)
    val snapshotReplica = if (metadata.snapshotReplica.isDefined) {
      metadata.snapshotReplica
    } else {
      redefinedSecondaryTargets.headOption
    }
    partitioning.put(token, TokenMetadata(numReplication, metadata.primaryTarget, snapshotReplica, redefinedSecondaryTargets))
  }
}

case class TokenMetadata(replication: Int, primaryTarget: Option[Target], snapshotReplica: Option[Target], secondaryTargets: Array[Target])

object Partitioning {
  type Token = String
  type Target = String
}