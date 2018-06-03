package com.miuv.util

import java.util.Date

trait Logging {

  private def now: String = {
    new Date().toString
  }

  def info(log: String): Unit = {
    println(s"INFO $now ${Thread.currentThread().getId} and ${this.getClass.toString} " + log)
  }

  def warn(log: String): Unit = {
    println(s"WARN $now " + log)
  }

  def error(log: String): Unit = {
    println(s"ERROR $now " + log)
  }

}
