package com.miuv.core

trait Encoder[T] {
  def serialize(data: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}
