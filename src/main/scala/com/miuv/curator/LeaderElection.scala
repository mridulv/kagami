package com.miuv.curator

import java.util.concurrent.Executors

import com.miuv.core.partitioner.LeaderPartitioner
import com.miuv.util.{Logging, StartStoppable, ZookeeperSequentialNode}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

class LeaderElection(curator: CuratorFramework,
                     connectionString: String,
                     nodeId: NodeId,
                     partitioners: ListBuffer[LeaderPartitioner])
  extends Logging {

  import LeaderElection._

  private lazy val nodeName = nodeId.nodeName
  private lazy val nodeNamePrefix = nodeName + "-"
  private lazy val logName = s"leader-$nodeName"

  private var _connected: Boolean = false
  private val curatorStateListener = new StateListener()

  private var childrenCache: Option[PathChildrenCache] = None

  private val childrenUpdateLock = new Object
  private var zkChildPath: String = null
  @volatile private var running: Boolean = true

  var currentLeader: String = null
  var electionCandidateCount: Int = 0
  var leadChangeCount: Int = 0

  def isLeader: Boolean = currentLeader == nodeName

  def enabled: Boolean = true

  _connected = true
  connected()
  curator.getConnectionStateListenable.addListener(curatorStateListener, Executors.newSingleThreadExecutor)


  def destroy(): Unit = {
    try {
      running = false
      if (isLeader) {
        info("Stopping leader during destroy()")
      }
      curator.delete().deletingChildrenIfNeeded().forPath(zkChildPath)
      childrenCache.foreach(_.close())
    } catch {
      case NonFatal(e) =>
        info(s"Error during destroy: couldn't stop leader or couldn't delete $zkChildPath from zookeeper" + e)
    } finally {
      this.synchronized {
        if (curator != null) {
          curator.getConnectionStateListenable.removeListener(curatorStateListener)
        }
      }
    }
  }

  def isConnected: Boolean = this.synchronized {
    _connected
  }

  def zkBasePath: String = basePath

  private def connectedEvent(): Unit = {
    info(s"[$logName] Connected to zookeeper at $connectionString")
    this.synchronized {
      _connected = true
    }
    connected()
  }

  private def disconnectedEvent(): Unit = {
    info(s"[$logName] Disconnected from zookeeper at $connectionString")
    this.synchronized {
      _connected = false
    }
    disconnected()
  }

  private def expiredEvent(): Unit = {
    info(s"[$logName] Session expired from zookeeper at $connectionString")
    this.synchronized {
      _connected = false
    }
    sessionExpired()
  }

  private def connected(): Unit = {

    info(s"Connected to zookeeper at $connectionString, base path $basePath - now registering.")

    if (Option(curator.checkExists().forPath(basePath)).isEmpty) {
      try {
        curator.create().creatingParentsIfNeeded().forPath(basePath)
      } catch {
        case e: NodeExistsException => warn(s"znode $basePath exists in zookeeper.")
      }
    }

    val originalChildren = curator.getChildren.forPath(basePath)
    cleanupExistingChildren(originalChildren.asScala)

    if (enabled && !nodeNamePrefix.startsWith("-")) {
      val pathPrefix = s"$basePath/$nodeNamePrefix"
      zkChildPath = curator.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(pathPrefix)

      info(s"Created ephemeral node $zkChildPath.")
    } else {
      info("Disabled, not creating ephemeral node.")
    }

    childrenCache = Some(new RichPathChildrenCacheBuilder(curator).
      withPathAddedCallback(path => addChildPath(path)).
      withPathRemovedCallback(path => removeChildPath(path)).
      buildAndStart(basePath))
    val children = curator.getChildren.forPath(basePath)
    childrenUpdated(children.asScala)
  }


  private def sessionExpired(): Unit = {
    info(s"Session expired from zookeeper at $connectionString, base path $basePath.")
    stopIfLeader()
  }

  private def disconnected(): Unit = {
    info(s"Disconnected from zookeeper at $connectionString, base path $basePath.")
    stopIfLeader()
  }

  private def addChildPath(path: String): Unit = {
    if (running) {
      info(s"Add child node path at $path.")
      val children = curator.getChildren.forPath(basePath).asScala
      childrenUpdated(children)
    }
  }

  private def removeChildPath(path: String): Unit = {
    if (running) {
      info(s"Remove child node path at $path.")
      val children = curator.getChildren.forPath(basePath).asScala
      childrenUpdated(children)
    }

  }

  private def childrenUpdated(children: Seq[String]): Unit = {

    childrenUpdateLock synchronized {

      electionCandidateCount = children.size

      if (electionCandidateCount < 1) {
        info(s"No children found in $basePath")
        return
      }

      val sortedChildren = children.map(new ZookeeperSequentialNode(_)).sortBy(_.sequenceId)
      val newLeader = sortedChildren.head
      if (newLeader.baseName != currentLeader) {
        leadChangeCount += 1
        if (newLeader.baseName == nodeName) {
          partitioners.foreach(_.doPartition(sortedChildren.map(_.baseName)))
          info("We are the leader! Starting to lead.")
        } else {
          info(s"${newLeader.baseName} is the leader!")
          stopIfLeader()
        }
        currentLeader = newLeader.baseName
      } else {
        if (newLeader.baseName == nodeName) {
          partitioners.foreach(_.doPartition(sortedChildren.map(_.baseName)))
          info("We are the leader! Starting to lead.")
        }
      }
    }
  }



  private def stopIfLeader(): Unit = {
    if (isLeader) {
      info("Stopping our leader.")
      currentLeader = null
    }
  }

  private def cleanupExistingChildren(originalChildren: Seq[String]): Unit = {
    val childrenToDelete = originalChildren.filter(_.startsWith(nodeNamePrefix))
    if (childrenToDelete.nonEmpty) {
      info(s"Removing ${childrenToDelete.size} old znodes starting with $nodeNamePrefix in base path $basePath.")
      childrenToDelete.map(c => s"$basePath/$c").foreach(path => curator.delete().forPath(path))

      val cleanedChildren = curator.getChildren.forPath(basePath).asScala
      val remainingChildren = cleanedChildren.filter(_.startsWith(nodeNamePrefix))
      if (remainingChildren.nonEmpty) {
        info(s"${remainingChildren.size} children could not be deleted: ${remainingChildren.mkString(",")}")
      }
    }
  }

  private def noop(): Unit = {}

  private class StateListener extends ConnectionStateListener {
    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      newState match {
        case ConnectionState.SUSPENDED => disconnectedEvent()
        case ConnectionState.LOST => expiredEvent()
        case ConnectionState.CONNECTED | ConnectionState.RECONNECTED => connectedEvent()
        case _ => noop()
      }
    }
  }
}

object LeaderElection {
  lazy val basePath = s"/basepath/leader"

  def isLeader(nodeId: NodeId, nodes: Seq[String]) = {
    val sortedChildren = nodes.map(new ZookeeperSequentialNode(_)).sortBy(_.sequenceId)
    val newLeader = sortedChildren.head
    nodeId.nodeName.equals(newLeader.baseName)
  }
}