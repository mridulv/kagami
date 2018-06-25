package com.miuv.examples

import com.miuv.core.KagamiFramework
import com.miuv.core.partitioner.Partitioning.Token
import com.miuv.kafka.producer.KagamiProducerIntermediate
import com.miuv.util.StringUtils

object AssemblyStartup {

  var simpleKagamiClient: SimpleKagamiClient = null

  def main(args: Array[String]): Unit = {
    val token = args(0)
    simpleKagamiClient = new SimpleKagamiClient
    val kagamiReplicatorWriter = new KagamiFramework()
      .addRequestListener(simpleKagamiClient)
      .init()

    val kagamiProducerIntermediate = kagamiReplicatorWriter.add(token, numReplication = 1)
    startWriting(token, kagamiProducerIntermediate)
  }

  def startWriting(token: Token, kagamiProducerIntermediate: KagamiProducerIntermediate): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          val str = StringUtils.randomString(5)
          simpleKagamiClient.addEntryToMap(token, str)
          val content = str.map(_.toByte).toArray
          kagamiProducerIntermediate.sendDataForReplication(content)
          Thread.sleep(5000)
        }
      }
    }).start()
  }
}
