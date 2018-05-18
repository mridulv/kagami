package com.miuv.curator

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.utils.ThreadUtils

class RichPathChildrenCacheBuilder(cl: CuratorFramework) {
  import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._

  private val NoOpFunc: PathChildrenCacheEvent => Unit = _ => {}

  private var cacheMode: Option[Boolean] = None
  private var initializedFunc: PathChildrenCacheEvent => Unit = NoOpFunc
  private var addedFunc: PathChildrenCacheEvent => Unit = NoOpFunc
  private var updatedFunc: PathChildrenCacheEvent => Unit = NoOpFunc
  private var removedFunc: PathChildrenCacheEvent => Unit = NoOpFunc
  private var threadPrefix: Option[String] = None

  /**
    * Sets the underlying path cache's cache mode.  See the Curator docs for more info.
    */
  def withDataCacheMode(mode: Boolean): RichPathChildrenCacheBuilder = {
    require(cacheMode.isEmpty, "You've already set the cacheMode")
    cacheMode = Some(mode)
    this
  }

  /**
    * Cache the content and stats of the zNode
    */
  def withCachingContent(): RichPathChildrenCacheBuilder = withDataCacheMode(mode = true)

  /**
    * Only cache the stats of the zNode, not the content
    */
  def withStatCachingOnly(): RichPathChildrenCacheBuilder = withDataCacheMode(mode = false)

  def withInitializedCallback(eventHandler: PathChildrenCacheEvent => Unit): RichPathChildrenCacheBuilder = {
    require(initializedFunc == NoOpFunc, "You've already set the initialized callback")
    initializedFunc = eventHandler
    this
  }

  def withAddedCallback(eventHandler: PathChildrenCacheEvent => Unit): RichPathChildrenCacheBuilder = {
    require(addedFunc == NoOpFunc, "You've already set the added callback")
    addedFunc = eventHandler
    this
  }

  def withUpdatedCallback(eventHandler: PathChildrenCacheEvent => Unit): RichPathChildrenCacheBuilder = {
    require(updatedFunc == NoOpFunc, "You've already set the updated callback")
    updatedFunc = eventHandler
    this
  }

  def withRemovedCallback(eventHandler: PathChildrenCacheEvent => Unit): RichPathChildrenCacheBuilder = {
    require(removedFunc == NoOpFunc, "You've already set the removed callback")
    removedFunc = eventHandler
    this
  }

  def withAddedOrUpdatedCallback(eventHandler: PathChildrenCacheEvent => Unit): RichPathChildrenCacheBuilder = {
    withAddedCallback(eventHandler)
    withUpdatedCallback(eventHandler)
  }

  def withThreadPrefix(prefix: String): RichPathChildrenCacheBuilder = {
    require(threadPrefix.isEmpty)
    threadPrefix = Some(prefix)
    this
  }

  def withPathAddedCallback(pathHandler: String => Unit): RichPathChildrenCacheBuilder =
    withAddedCallback(pathHandlerToEventHandler(pathHandler))
  def withPathUpdatedCallback(pathHandler: String => Unit): RichPathChildrenCacheBuilder =
    withUpdatedCallback(pathHandlerToEventHandler(pathHandler))
  def withPathRemovedCallback(pathHandler: String => Unit): RichPathChildrenCacheBuilder =
    withRemovedCallback(pathHandlerToEventHandler(pathHandler))
  def withPathAddedOrUpdatedCallback(pathHandler: String => Unit): RichPathChildrenCacheBuilder =
    withAddedOrUpdatedCallback(pathHandlerToEventHandler(pathHandler))

  private[this] def pathHandlerToEventHandler(pathHandler: String => Unit): PathChildrenCacheEvent => Unit = { event =>
  {
    val path = event.getData.getPath
    pathHandler(path)
  }
  }

  def build(path: String): PathChildrenCache = {
    val cache = threadPrefix match {
      case Some(prefix) =>
        // ThreadUtils.newThreadFactory is what it uses by default if not provided
        val threadFactory = ThreadUtils.newThreadFactory(s"PathChildrenCache-$threadPrefix")
        new PathChildrenCache(cl, path, cacheMode.getOrElse(true), threadFactory)
      case None =>
        new PathChildrenCache(cl, path, cacheMode.getOrElse(true))
    }

    cache.getListenable.addListener(new PathChildrenCacheListener {
      override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
        event.getType match {
          case CHILD_ADDED => addedFunc(event)
          case CHILD_REMOVED => removedFunc(event)
          case CHILD_UPDATED => updatedFunc(event)
          case INITIALIZED => initializedFunc(event)
          case _ => {}
        }
      }
    })

    cache
  }

  def buildAndStart(path: String): PathChildrenCache = {
    val cache = build(path)
    val startMode = if (initializedFunc == NoOpFunc) {
      PathChildrenCache.StartMode.NORMAL
    } else {
      PathChildrenCache.StartMode.POST_INITIALIZED_EVENT
    }
    cache.start(startMode)
    cache
  }
}

