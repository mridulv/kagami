package com.miuv.core.partitioner

import com.miuv.core.ZookeeperStore
import org.apache.curator.framework.listen.ListenerContainer
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.scalatest.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ZookeeperStoreTestUtil extends ZookeeperStore[Partitioning] with MockitoSugar with Matchers {

  val listeners: mutable.ArrayBuffer[NodeCacheListener] = new ArrayBuffer[NodeCacheListener]()
  var partitioning: Partitioning = null

  override protected lazy val nodeCache: NodeCache = {
    val cache = mock[NodeCache]
    val listenerContainer = mock[ListenerContainer[NodeCacheListener]]
    when(cache.getListenable).thenReturn(listenerContainer)
    when(listenerContainer.addListener(any())).thenAnswer(new Answer[Unit]() {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        val mock = invocation.getMock
        val listener = args(0).asInstanceOf[NodeCacheListener]
        listeners.append(listener)
      }
    })
    cache
  }

  override def store(entry: Partitioning): Unit = {
    partitioning = entry
  }

  override def load(): Partitioning = {
    partitioning
  }

  def invokeListeners(): Unit = {
    listeners.foreach(_.nodeChanged())
  }
}