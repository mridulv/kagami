package com.miuv.curator

import com.miuv.util.StringUtils

import scala.util.Random

class NodeId(val nodeName: String = StringUtils.randomString(10)) {
  val nodeId: Int = Random.nextInt(10)
}
