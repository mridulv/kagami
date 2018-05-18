package com.miuv.curator

import scala.util.Random

class NodeId {
  val nodeName = Random.nextString(10)
  val nodeId = Random.nextInt(10)
}
