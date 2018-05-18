package com.miuv.curator

/**
  * Created by mridul on 22/04/18.
  */
trait Logging {

  def info(log: String): Unit = {
    print("INFO " + log)
  }

  def warn(log: String): Unit = {
    print("WARN " + log)
  }

  def error(log: String): Unit = {
    print("ERROR " + log)
  }

}
