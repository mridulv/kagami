package com.miuv.curator

import java.io.File

import com.miuv.core.partitioner.{LeaderPartitioner, ZookeeperPartitioningStore}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class LeaderElectionTest
  extends FlatSpec
    with Matchers
    with MockitoSugar
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Eventually with SpanSugar {

  val connectString = "localhost:2181"
  var server: TestingServer = _
  var curatorFramework: CuratorFramework = _

  "LeaderElection" should "make only one of the nodes as leader" in {
    val zookeeperPartitioningStore = mock[ZookeeperPartitioningStore]
    val leaderElectionMap = (1 to 10).map(elem => {
      val nodeId = new NodeId()
      val stubPartitioner = new StubPartitioner(zookeeperPartitioningStore, nodeId)
      stubPartitioner -> new LeaderElection(curatorFramework, connectString, nodeId, ListBuffer(stubPartitioner))
    }).toSet

    val leaderElection = collection.mutable.Map(leaderElectionMap.toSeq: _*)

    (1 to 5).foreach(elem => {
      eventually(timeout(1.minute)) {
        leaderElection.map(_._1.currentInstances == leaderElection.size)
        leaderElection.count(_._1.count > 0) should be(1)
        val leader = leaderElection.filter(_._1.count > 0).head
        val currentInstances = leaderElection.map(_._1.nodeId.nodeName).toSeq.sorted
        leader._1.currentInstances should be(currentInstances)
        leader._2.destroy()
        Thread.sleep(500)
        leaderElection.remove(leader._1)
      }
    })

    leaderElection.size should be (5)
    leaderElection.count(_._1.count > 0) should be (1)
  }

  override def afterAll(): Unit = {
    curatorFramework.close()
  }

  override def beforeAll(): Unit = {
    server = new TestingServer(2181, new File("/tmp"))
    curatorFramework = CuratorFrameworkFactory.builder
      .connectString(server.getConnectString)
      .sessionTimeoutMs(200)
      .retryPolicy(new RetryNTimes(3, 1000))
      .build
    curatorFramework.start()
  }
}

class StubPartitioner(zookeeperPartitioningStore: ZookeeperPartitioningStore, val nodeId: NodeId)
  extends LeaderPartitioner(zookeeperPartitioningStore) {

  var count = 0L
  var currentInstances: Seq[String] = null

  override def doPartition(currentTargets: Seq[String]): Unit = {
    count += 1
    currentInstances = currentTargets.sorted
  }
}
