package com.miuv.examples

import java.io.{File, FileOutputStream}

import com.oracle.tools.packager.IOUtils

import scala.collection.mutable
import scala.util.Random

object SerDeserSnapshot {
  type Snapshot = mutable.Map[String, Int]

  val sep1 = "::"
  val sep2 = ":"
  val metadataSep = "--"
  val targetsSep = ";"

  def addEntry(snapshot: Snapshot): String = {
    val name = Random.alphanumeric(10).toString
    val f = new FileOutputStream(name)
    val keys = snapshot.keys.mkString(sep2)
    val values = snapshot.values.mkString(sep2)
    val bytes = (keys + sep1 + values).map(_.toByte).toArray
    f.write(bytes)
    f.flush()
    f.close()
    name
  }

  def getEntry(key: String): Snapshot  = {
    val bytes = IOUtils.readFully(new File(key))
    val snapshot = bytes.map(_.toChar).toString
    val keys = snapshot.split(sep1)(0).split(sep2)
    val values = snapshot.split(sep1)(1).split(sep2).map(_.toInt)
    collection.mutable.Map(keys.zip(values).toSeq: _*)
  }

}