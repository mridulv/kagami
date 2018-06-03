package com.miuv.examples

import com.miuv.core.KagamiFramework

object AssemblyStartup {

  def main(args: Array[String]): Unit = {
    new SimpleKagamiClient(kagamiFramework = new KagamiFramework(), args(0))
  }
}
