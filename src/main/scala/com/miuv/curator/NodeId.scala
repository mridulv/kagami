package com.miuv.curator

import scala.util.Random

class NodeId(val nodeName: String = Random.nextString(10)) {
  val nodeId: Int = Random.nextInt(10)
}
