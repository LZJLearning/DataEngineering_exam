# Kafka Cluster Fault Tolerance Test

## 1. Objective
The goal of this test is to demonstrate the high availability and fault tolerance of the 3-node KRaft Kafka cluster. We will observe how the cluster handles the sudden failure of a broker by triggering a Leader re-election and updating the In-Sync Replicas (ISR) list.

## 2. Initial State (Before Failure)
Before stopping any broker, the `sensor-events` topic distributes its leaders evenly across the 3 brokers.

**Command executed:**
bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
Topic: sensor-events    TopicId: IByrHQoaQSmyoUfzmhlQtQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: sensor-events    Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
        Topic: sensor-events    Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
        Topic: sensor-events    Partition: 2    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
Broker 2 is the Leader for Partition 0. All 3 brokers are in the ISR for all partitions.

## 3. Simulating Node Failure
We simulated a node crash by stopping kafka2:
docker stop kafka2

## 4. Final State (After Failure)
Immediately after kafka2 goes down, we describe the topic again to observe the cluster's self-healing mechanism.
Command executed:
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
Output trace:
Topic: sensor-events    TopicId: IByrHQoaQSmyoUfzmhlQtQ PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Topic: sensor-events    Partition: 0    Leader: 3       Replicas: 2,3,1 Isr: 3,1
        Topic: sensor-events    Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 3,1
        Topic: sensor-events    Partition: 2    Leader: 1       Replicas: 1,2,3 Isr: 1,3

## 5. Analysis & Conclusion
The test successfully demonstrates fault tolerance:
Leader Re-election: Partition 0's leader was originally Broker 2. When it crashed, the cluster immediately elected Broker 3 as the new leader.
ISR Shrinkage: Broker 2 was removed from the Isr (In-Sync Replicas) list for all partitions (e.g., Partition 0's ISR changed from 2,3,1 to 3,1).
Availability Maintained: Because we configured min.insync.replicas=2, and the ISR still contains 2 nodes (3 and 1), the cluster can still safely accept new messages with acks=all without any downtime.