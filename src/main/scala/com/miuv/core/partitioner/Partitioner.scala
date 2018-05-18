package com.miuv.core.partitioner

trait Partitioner[Target, Token] {

  def partition(target: Seq[Target], token: Seq[Token]): Map[Target, Seq[Token]]

}
