# Overview: Building a Fault-Tolerant Key-Value Store 

In this MP, you will be building a fault-tolerant key-value store. We are providing you with the same 
template provided for C3 Part 1 Programming Assignment (Membership Protocol), along with an almostcomplete implementation of the key-value store, and a set of tests (which don’t pass on the released code). This means first you need to use your working version of Membership Protocol from C3 Part 1 Programming Assignment and integrate it with this assignment. Then you need to fill in some key methods to complete the implementation of the fault-tolerant key-value store and pass all the tests. 

# Functionalities

Concretely, you will be implementing the following functionalities: 

1. A key-value store supporting CRUD operations (Create, Read, Update, Delete). 
2. Load-balancing (via a consistent hashing ring to hash both servers and keys). 
3. Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key). 
4. Quorum consistency level for both reads and writes (at least two replicas). 
5. Stabilization after failure (recreate three replicas after failure).

# Notes

Please note that the membership list may be stale at nodes! This models the reality in distributed systems. So, code that you write must be aware of this. Also, when you react to a failure (e.g., by re-replicating a key whose replica failed), make sure that there is no contention among the would-be replicas. Do not over-replicate keys!
