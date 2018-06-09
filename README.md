# Kagami

[![Build Status](https://travis-ci.org/mridulv/kagami.svg?branch=master)](https://travis-ci.org/mridulv/kagami)

Availability is one of the key concerns for any of today's microservices. Providing availability in any stateful service is not straightforward and requires a lot of work. Replication is one of the solutions used world wide by developers to provide availability for their microservices. But getting Replication right is one tough problem as it requires a lot of corner cases handling.

Kagami (*japanese name for mirror*) solves this problem by using kafka as a commit log. 
- Kagami offers a simple interface which needs to be implemented by the users for receiving replicated data.
- Kagami takes care of replicating the state/writes from one node to some other node. 
- Kagami makes sure that these replicas are evenly distributed across nodes.
- Kagami also support iterative snapshotting and replica reconstruction required in cases of node loss.

For more details, see [this](https://miuv.blog/2018/04/16/building-replicated-distributed-systems-with-kafka/)

### Requirements
- Curator Framework >= 2.8 version
- Kafka >= 1.1.0

### Getting Started

You need to have zookeeper ( *localhost:2181* ) and kafka ( *localhost:9092* ) running on your machine

```
git clone https://github.com/mridulv/kagami.git
cd kagami
mvn package
```

```
java -jar target/kagami-1.0-SNAPSHOT-jar-with-dependencies.jar token1 &
java -jar target/kagami-1.0-SNAPSHOT-jar-with-dependencies.jar token2 &
```

### Overview

Here is a basic architecture explaining the internals of kagamiFramework

<img src="https://mypersonalmusingsblog.files.wordpress.com/2018/06/blank-diagram-page-1-66-e1528296740620.jpeg"     alt="Overview" style="float: left; margin-right: 10px;" width="700" height="350"/> 

### Usage

For using this library you just need to implement the **kagamiClient** interface ( see for example **SimpleKagamiClient** ), and the kagami library will make sure that all the subsequent writes which are happening on this node ( for a particular token ) are replicated on another available node.

```
  def receiveReplicatedData(token: Token, data: Array[Byte])
```

For example implementation, have a look at [SimpleKagamiClient](https://github.com/mridulv/kagami/blob/master/src/main/scala/com/miuv/examples/SimpleKagamiClient.scala)
