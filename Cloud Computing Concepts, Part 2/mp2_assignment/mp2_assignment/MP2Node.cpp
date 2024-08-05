/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

#define TIMEOUT 40

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	trans_ht = new map<int, Transaction>();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete trans_ht;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
  if (curMemList < ring || ring < curMemList || curMemList.size() != ring.size()) {
    change = true;
    ring = curMemList;
  }

  if (change) { stabilizationProtocol(); }

}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
  // 1) Constructs the message
  int transID = ++g_transID;
  int timestamp = par->getcurrtime();
  Address fromAddress = memberNode->addr;

  trans_ht->insert({transID, {0, 0, CREATE, key, value, par->getcurrtime()}});


  Message primary_msg = Message(transID, fromAddress, CREATE, key, value, PRIMARY);
  Message secondary_msg = Message(transID, fromAddress, CREATE, key, value, SECONDARY);
  Message tertiary_msg = Message(transID, fromAddress, CREATE, key, value, TERTIARY);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (i==0) { emulNet->ENsend(&fromAddress, &nodes[0].nodeAddress, primary_msg.toString()); }
    if (i==1) { emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, secondary_msg.toString()); }
    if (i==2) { emulNet->ENsend(&fromAddress, &nodes[2].nodeAddress, tertiary_msg.toString()); }
  }

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
  // 1) Constructs the message
  int transID = ++g_transID;
  Address fromAddress = memberNode->addr;

  trans_ht->insert({transID, {0, 0, READ, key, "", par->getcurrtime()}});

  Message msg = Message(transID, fromAddress, READ, key);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (i==0) { emulNet->ENsend(&fromAddress, &nodes[0].nodeAddress, msg.toString()); }
    if (i==1) { emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, msg.toString()); }
    if (i==2) { emulNet->ENsend(&fromAddress, &nodes[2].nodeAddress, msg.toString()); }
  }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
  // 1) Constructs the message
  int transID = ++g_transID;
  int timestamp = par->getcurrtime();
  Address fromAddress = memberNode->addr;

  trans_ht->insert({transID, {0, 0, UPDATE, key, value, par->getcurrtime()}});


  Message primary_msg = Message(transID, fromAddress, UPDATE, key, value, PRIMARY);
  Message secondary_msg = Message(transID, fromAddress, UPDATE, key, value, SECONDARY);
  Message tertiary_msg = Message(transID, fromAddress, UPDATE, key, value, TERTIARY);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (i==0) { emulNet->ENsend(&fromAddress, &nodes[0].nodeAddress, primary_msg.toString()); }
    if (i==1) { emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, secondary_msg.toString()); }
    if (i==2) { emulNet->ENsend(&fromAddress, &nodes[2].nodeAddress, tertiary_msg.toString()); }
  }

}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
  // 1) Constructs the message
  int transID = ++g_transID;
  Address fromAddress = memberNode->addr;

  trans_ht->insert({transID, {0, 0, DELETE, key, "", par->getcurrtime()}});

  Message msg = Message(transID, fromAddress, DELETE, key);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
	  emulNet->ENsend(&fromAddress, &nodes[i].nodeAddress, msg.toString());
  }

}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
  ht->create(key, value);
  return ht->count(key) > 0;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
  return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE APreplica << std::endl * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
  return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
  return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the messagtypes here */
    Message msg = Message(message);
   
    // CREATE, READ, UPDATE, DELETE, REPLY, READREPLY
    switch (msg.type) {
      case CREATE: handleCreateMsg(msg); break;
      case READ: handleReadMsg(msg); break;
      case UPDATE: handleUpdateMsg(msg); break;
      case DELETE: handleDeleteMsg(msg); break;
      case REPLY: handleReplyMsg(msg); break;
      case READREPLY: handleReadReplyMsg(msg); break;
    } 

	}
  
  // Handle timeout
  map<int, Transaction>& htt = *trans_ht;
  vector<int> timeoutedTrans;
  for (auto const & [transID, tran] : htt) {
    if (tran.timestamp+TIMEOUT< par->getcurrtime()) {
      switch (tran.messageType) {
        case CREATE: log->logCreateFail(&memberNode->addr, true, transID, tran.key, tran.value); break; 
        case READ: log->logReadFail(&memberNode->addr, true, transID, tran.key); break; 
        case UPDATE: log->logUpdateFail(&memberNode->addr, true, transID, tran.key, tran.value); break; 
        case DELETE: log->logDeleteFail(&memberNode->addr, true, transID, tran.key); break; 
      }
      timeoutedTrans.emplace_back(transID);
    }
  }
  for (unsigned int i=0; i<timeoutedTrans.size(); i++) {
    trans_ht->erase((int)timeoutedTrans[i]);
  }


	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

void MP2Node::handleReplyMsg(Message msg) {

	if (msg.transID == -1 || !trans_ht->count(msg.transID)) {
    if (msg.transID != -1 && !trans_ht->count(msg.transID)) {
      std::cout << "READ timeout: " << msg.transID << std::endl;
    }
		// Key not found
		return;
	}

  Transaction tran = trans_ht->at(msg.transID);

  if (msg.success) {
    tran.successCount++;
  } else {
    tran.failCount++;
  }

  trans_ht->at(msg.transID) = tran;

  if (tran.successCount > 1) {
    switch (tran.messageType) {
      case CREATE: log->logCreateSuccess(&memberNode->addr, true, msg.transID, tran.key, tran.value); break; 
      case UPDATE: log->logUpdateSuccess(&memberNode->addr, true, msg.transID, tran.key, tran.value); break; 
      case DELETE: log->logDeleteSuccess(&memberNode->addr, true, msg.transID, tran.key); break; 
    }
    trans_ht->erase(msg.transID);
  } 

  if (tran.failCount > 1 || tran.timestamp+TIMEOUT< par->getcurrtime()) {
    switch (tran.messageType) {
      case CREATE: log->logCreateFail(&memberNode->addr, true, msg.transID, tran.key, tran.value); break; 
      case UPDATE: log->logUpdateFail(&memberNode->addr, true, msg.transID, tran.key, tran.value); break; 
      case DELETE: log->logDeleteFail(&memberNode->addr, true, msg.transID, tran.key); break; 
    }
    trans_ht->erase(msg.transID);
  }

  return;
}

void MP2Node::handleReadReplyMsg(Message msg) {
	if (msg.transID == -1 || !trans_ht->count(msg.transID)) {
		// Key not found
		return;
	}

  Transaction tran = trans_ht->at(msg.transID);

  if (msg.value.empty()) {
    tran.failCount++;
  } else {
    tran.successCount++;
  }
  tran.value = msg.value;
  trans_ht->at(msg.transID) = tran;

  if (tran.successCount > 1) {
    log->logReadSuccess(&memberNode->addr, true, msg.transID, tran.key, tran.value);
    trans_ht->erase(msg.transID);
  } 
  if (tran.failCount > 1 || tran.timestamp+TIMEOUT< par->getcurrtime()) {
    log->logReadFail(&memberNode->addr, true, msg.transID, tran.key);
    trans_ht->erase(msg.transID);
  }
}


void MP2Node::handleCreateMsg(Message msg) {
  bool isCreated = createKeyValue(msg.key, msg.value, msg.replica);

  if (msg.transID != -1) { 
    if (isCreated) {
      log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
    } else {
      log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
    }
  }

  Message reply = Message(msg.transID, memberNode->addr, REPLY, isCreated); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
}

void MP2Node::handleReadMsg(Message msg) {
  string value = readKey(msg.key);

  if (msg.transID != -1) { 
    if (value.empty()) {
      log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
    } else {
      log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, value);
    }
  }

  Message reply = Message(msg.transID, memberNode->addr, value); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
}

void MP2Node::handleUpdateMsg(Message msg) {
  bool isUpdated = updateKeyValue(msg.key, msg.value, msg.replica);
  
  if (msg.transID != -1) { 
    if (isUpdated) {
      log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
    } else {
      log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
    }
  }

  Message reply = Message(msg.transID, memberNode->addr, REPLY, isUpdated); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
}

void MP2Node::handleDeleteMsg(Message msg) {
  string value = readKey(msg.key);
  bool isDeleted = deletekey(msg.key);

  if (msg.transID != -1) { 
    if (isDeleted) {
      log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
    } else {
      log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
    }
  }

  Message reply = Message(msg.transID, memberNode->addr, REPLY, isDeleted); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

  Address fromAddress = memberNode->addr;

  for (auto const & [key, value]: ht->hashTable) {
    // Copy keys to new replica
    vector<Node> replicas = findNodes(key);
    for (unsigned i=0; i<replicas.size(); i++) {
      Message msg = Message(-1, fromAddress, CREATE, key, value, PRIMARY);
      if (i==1) { msg.replica = SECONDARY; }
      if (i==2) { msg.replica = TERTIARY; }
      emulNet->ENsend(&fromAddress, &replicas[i].nodeAddress, msg.toString());
    }
  }

  return;
}
