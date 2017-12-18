# Distributed Log Aggregator - Design and Implementation

### 1. Overview
The solution for a centralized log system is described in two parts :

- **Design of a scalable solution architecture** - Meets the pointed requirements
- **Proof of concept (POC)** - Implementation of working agent and service applications

### 2. Design of a scalable solution architecture


#### 2.1. Short description
   The solution illustrated below uses Kafka as a messaging system (publish-subscribe in this case) and uses the agents as producers and the services as consumers. The log files are tailed and each line is published in a message to a single  Kafka topic. The service subscribes to topics and recreates the log file line by line.

#### 2.2.  Diagram of components :

![Design](https://raw.githubusercontent.com/yosstef/datastax_agent_producer/master/DatastaxTest.png "Design")

#### 2.3. Requirements coverage
Below are listed the task requirement with description how are they met:

- ##### “Distributed log aggregation”
Distribution is allowed/achieved by using a messaging system that is capable of aggregation of messages from thousands of producers.

- ##### “Tailing a given log file”
Each agent can be configured to tail multiple log files concurrently and also concurrently send messages batched through a single producer connection.

- ##### “Delivering contents in a timely manner”
Using Kafka as a dedicated messaging system guarantees fast delivery even with big volume of streamed content.

- ##### “Accepting chunked log contents from the client and stitching it together” + 
- ##### “Preserve the original order of entries (lines)”

Log files are read line by line and published to a Kafka topic. Topics consist of only one partition. Each published message needs to be committed/acknowledged by the broker. On the other side the subscribed consumer reads and commits that a message is consumed in the same order by keeping offset within the topic’s partition. This process is orchestrated using Zookeeper for communicating metadata between the producers, clusters and consumers. It guarantees order preservation and consistency.

- ##### “The server can accept log files delivered from multiple concurrent agents” + 
- ##### “Creating a copy of the original log” +
- ##### “Make sure that the resulting log files on the server don’t contain any duplicate lines”

On the side of Kafka there is a replication mechanism to guarantee a message will not be lost ( there is also the mechanism of receiving  acknowledgement from the configured number of partition followers after message is published)
The server side (service) listens on each topic concurrently - there is a thread for each topic. The messages are consumed in a batch together, written in the same thread and just then committed as consumed before the thread is put into sleeping state - this is done intentionally to guarantee that every line is written to the file (just once). 
The consuming service writes to a configured local file with the same name line by line, so it is effectively a copy of the original file.

- ##### “Avoid losing parts of a log due to network unreliability” +
- ##### “Consider strategies for coping with back pressure and overflow”
Several strategies are in place to meet the above requirements: There is a replication mechanism on the Kafka side and also the producing agents require acknowledgement from the brokers. There is also a buffer on the side of the producer which allows the tailing thread not to be blocked by the i/o process, but also store the messages until network problems are resolved. The producer addiotionally tries to batch messages to optimise the network connection. 
	All the above strategies combined achieve reliability and protection against overflow and back pressure on the side of the producer.
	On the side of the consumer the problem is mostly solved by the Kafka clusters as medium short term storage and also by allowing coordination of the read and committed messages using Zookeeper metadata.

- ##### “How would you change the protocol to allow each agent to aggregate multiple log files concurrently?”
The consumer is multi-threaded and is capable of aggregating  multiple log files concurrently by listening to multiple topics and receiving messages in a batch. 

- ##### “How would you design the system to allow aggregation from hundreds of thousands of agents?”
	That massive number of streamed data producers is what messaging systems like Kafka are built for - with the addition of multiple Kafka brokers in clusters the volume of data can be handled reliably.
Using multiple Kafka brokers solves part of the problem, even though a single cluster can handle tens of thousands of messages per second (that performance is considered to be kept in a linear progression with the addition of each new broker within the cluster, excluding the initial load during replication) . But that big number of agents most likely creates another constraint/problem - groups of agents would be geographically distanced from each other. This would make the use of a single cluster bad idea because Kafka brokers would communicate between each other with higher network latency (out-of-sync followers) . 
The best solution is to create many clusters that are closer to the groups of agents, but still replicate between each other. This way the consumers can aggregate messages from the closest cluster reliably even for a massive data load in the system.
A 3rd problem arises on the consumer side - Kafka is fit  only as a short term memory storage, but not as a big data  persistent storage. We would need to have multiple consumers, too. They can benefit from the addition of a distributed persistent storage - a database such as Cassandra can be used to store big volume of data, instead of writing it locally - this way we can put consumers closer to the producers/messaging clusters, but aggregate the data in a system that allows fast replicated access to the data.
			
	
### 3. POC implementation explanation

#### 3.1. Short implementation description / context
The proof of concept implementation demonstrates working applications for agent and service. They act a Kafka producer and consumer respectively. 
	**The used stack is short :**
Java SE 8
Kafka
Apache Commons I/O
JUnit
Log4j 2
Maven
		
***Both applications are packaged to a runnable jar file.***

#### 3.2. Agent (producer) implementation
The application creates multiple threads to tail the configured log files. All threads use a single thread-safe Kafka producer that is used to send messages to the Kafka brokers. 

#### 3.3. Service (consumer) implementation
The service uses multiple threads to subscribe to the configured Kafka topics. Internally they initialise Kafka consumer which listens and receives batches of messages. When new messages are received they are first written to a log file and then a commit confirmation is sent to the Kafka brokers.

