package com.miuv.examples

import com.miuv.core.KagamiFramework

/**
  * Created by mridul on 19/05/18.
  */
object AssemblyStartup {

  def main(args: Array[String]): Unit = {
    new SimpleKagamiClient(kagamiFramework = new KagamiFramework())
  }

}
