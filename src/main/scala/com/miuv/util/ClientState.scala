package com.miuv.util

object ClientState extends Enumeration {
  type ClientState = Value
  val Running, NotStarted, Stopped = Value
}
