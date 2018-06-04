# Kagami
Availability is one of the key concerns for any of the today's microservices. 
Providing availability in any stateless service is somewhat easy as they don't have any state 
so services going up and down is not that big of an issue. But for stateful systems, we 
have to do a lot for providing availability. Replication is one of the solutions 
used world wide by developers to provide availability for their microservices. But replication is a 
difficult problem to solve as this involves careful interactions between the replicas and making sure 
that there is minimal performance impacted at the cost of availability. 

Kagami (*japanese name for mirror*) solves this problem by using kafka as a commit log. Kagami offers 
a simple interface which needs to be implemented by the developers.   


### Installation

You need to have zookeeper and kafka running on your machine

```
git clone https://github.com/mridulv/kagami.git
cd kagami
mvn package
java -jar target/kagami-1.0-SNAPSHOT-jar-with-dependencies.jar token1
```


