package com.miuv.util

import java.util.Date

trait Logging {

  private def now: String = {
    new Date().toString
  }

  def info(log: String): Unit = {
    print(s"INFO $now" + log)
  }

  def warn(log: String): Unit = {
    print(s"WARN $now" + log)
  }

  def error(log: String): Unit = {
    print(s"ERROR $now" + log)
  }

}
