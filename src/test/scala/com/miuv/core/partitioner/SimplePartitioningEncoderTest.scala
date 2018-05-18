package com.miuv.core.partitioner

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.verb.ShouldVerb

import _root_.scala.language.implicitConversions
import scala.collection.mutable

//@RunWith(classOf[JUnitRunner])
class SimplePartitioningEncoderTest { //extends FlatSpec with MockitoSugar with Matchers with ShouldVerb {

  //val simplePartitioningEncoder = new SimplePartitioningEncoder
  //val targets = Seq("node-1", "node-2", "node-3")
  //val tokens = Seq("token1", "token2", "token3")
  //val partitioningData = mutable.Map(
  //  tokens(0) -> TokenMetadata(1, targets(0), Array(targets(1), targets(2))),
  //  tokens(1) -> TokenMetadata(1, targets(1), Array(targets(2), targets(0))),
  //  tokens(2) -> TokenMetadata(1, targets(2), Array(targets(0), targets(1)))
  //)

  //"SimplePartitioningEncoder" should "encode the values in proper way" in {
  //  val partitioning = new Partitioning(partitioningData)
  //  val resultantPartitioning = simplePartitioningEncoder.deserialize(simplePartitioningEncoder.serialize(partitioning))
  //  assert(resultantPartitioning.partitioning.keys.toSeq == tokens)
  //}

}
