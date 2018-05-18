package com.miuv.util

trait StartStoppable {

  // Let subclasses control strictness.

  protected var strict = true

  // Set in stone.

  private var started = false

  final def isStarted: Boolean = started

  final def start(): Unit = {
    if (!started) {
      doStart()
      started = true
    } else if (strict) {
      throw new IllegalStateException("Already started.")
    }
  }

  final def stop(): Unit = {
    if (started) {
      doStop()
      started = false
    } else if (strict) {
      throw new IllegalStateException("Already stopped.")
    }
  }

  // Subclasses implement these.

  protected def doStart(): Unit = {}
  protected def doStop(): Unit = {}
}

