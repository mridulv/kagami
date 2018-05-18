package com.miuv.util

import java.util.regex.Pattern

class ZookeeperSequentialNode(nodeName: String) {
  require(nodeName != null, "Node name is null!")
  private val matcher = Pattern.compile("(.*)\\-(\\d{10})").matcher(nodeName)
  require(matcher.matches(), "%s does not match pattern!".format(nodeName))
  val baseName = matcher.group(1)
  val sequenceId = matcher.group(2).toLong
}
