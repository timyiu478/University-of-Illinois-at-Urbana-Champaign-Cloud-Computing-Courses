/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	sec_ht = new HashTable();
	ter_ht = new HashTable();
	trans_ht = new map<int, Transaction>();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete sec_ht;
	delete ter_ht;
	delete trans_ht;
	delete memberNode;
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
  // change = curMemList == ring;
  // if (!change) {
    // return;
  // }

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

  Entry primary_entry = Entry(value, timestamp, PRIMARY); 
  Entry secondary_entry = Entry(value, timestamp, SECONDARY); 
  Entry tertiary_entry = Entry(value, timestamp, TERTIARY); 

  Message primary_msg = Message(transID, fromAddress, CREATE, key, primary_entry.convertToString(), PRIMARY);
  Message secondary_msg = Message(transID, fromAddress, CREATE, key, secondary_entry.convertToString(), SECONDARY);
  Message tertiary_msg = Message(transID, fromAddress, CREATE, key, tertiary_entry.convertToString(), TERTIARY);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (i==0) { emulNet->ENsend(&fromAddress, &nodes[0].nodeAddress, primary_msg.toString()); }
    if (i==1) { emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, secondary_msg.toString()); }
    if (i==2) { emulNet->ENsend(&fromAddress, &nodes[2].nodeAddress, tertiary_msg.toString()); }
  }

  // 4) Add transaction
  trans_ht->insert({transID, {0, 0, CREATE}});
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

  Message msg = Message(transID, fromAddress, READ, key);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
	  emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, msg.toString());
  }
  // 4) Add transaction
  trans_ht->insert({transID, {0, 0, READ}});
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

  Entry primary_entry = Entry(value, timestamp, PRIMARY); 
  Entry secondary_entry = Entry(value, timestamp, SECONDARY); 
  Entry tertiary_entry = Entry(value, timestamp, TERTIARY); 

  Message primary_msg = Message(transID, fromAddress, UPDATE, key, primary_entry.convertToString(), PRIMARY);
  Message secondary_msg = Message(transID, fromAddress, UPDATE, key, secondary_entry.convertToString(), SECONDARY);
  Message tertiary_msg = Message(transID, fromAddress, UPDATE, key, tertiary_entry.convertToString(), TERTIARY);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (i==0) { emulNet->ENsend(&fromAddress, &nodes[0].nodeAddress, primary_msg.toString()); }
    if (i==1) { emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, secondary_msg.toString()); }
    if (i==2) { emulNet->ENsend(&fromAddress, &nodes[2].nodeAddress, tertiary_msg.toString()); }
  }

  // 4) Add transaction
  trans_ht->insert({transID, {0, 0, UPDATE}});
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

  Message msg = Message(transID, fromAddress, DELETE, key);

  // 2) Finds the replicas of this key
  vector<Node> nodes = findNodes(key);

  // 3) Sends a message to the replica
  for (unsigned i = 0; i < nodes.size(); i++) {
	  emulNet->ENsend(&fromAddress, &nodes[1].nodeAddress, msg.toString());
  }

  // 4) Add transaction
  trans_ht->insert({transID, {0, 0, DELETE}});
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
  if (replica == PRIMARY) {
    return ht->create(key, value);
  }
  if (replica == SECONDARY) {
    return sec_ht->create(key, value);
  }
  if (replica == TERTIARY) {
    return ter_ht->create(key, value);
  }
  return false;
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
  string value;

  value = ht->read(key);
  if (value != "") {
    return value;
  }

  value = sec_ht->read(key);
  if (value != "") {
    return value;
  }

  value = ter_ht->read(key);
  if (value != "") {
    return value;
  }

  return "";
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
  if (replica == PRIMARY) {
    return ht->update(key, value);
  }
  if (replica == SECONDARY) {
    return sec_ht->update(key, value);
  }
  if (replica == TERTIARY) {
    return ter_ht->update(key, value);
  }
  return false;
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
  if (ht->deleteKey(key)) { return true; }
  if (sec_ht->deleteKey(key)) { return true; }
  if (ter_ht->deleteKey(key)) { return true; }
  return false;
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
		 * Handle the message types here
		 */
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

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

void MP2Node::handleReplyMsg(Message msg) {
  Transaction tran = trans_ht->at(msg.transID);

  if (msg.success) {
    tran.successCount++;
  } else {
    tran.failCount++;
  }

  if (tran.successCount > 1) {
    switch (msg.type) {
      case CREATE: log->logCreateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value); break; 
      case READ: log->logReadSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value); break; 
      case UPDATE: log->logUpdateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value); break; 
      case DELETE: log->logDeleteSuccess(&memberNode->addr, true, msg.transID, msg.key); break; 
    }
    trans_ht->erase(msg.transID);
  } 

  if (tran.failCount > 1) {
    switch (msg.type) {
      case CREATE: log->logCreateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value); break; 
      case READ: log->logReadFail(&memberNode->addr, true, msg.transID, msg.key); break; 
      case UPDATE: log->logUpdateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value); break; 
      case DELETE: log->logDeleteFail(&memberNode->addr, true, msg.transID, msg.key); break; 
    }
    trans_ht->erase(msg.transID);
  }
}

void MP2Node::handleReadReplyMsg(Message msg) {
  Transaction tran = trans_ht->at(msg.transID);

  if (msg.value == "") {
    tran.failCount++;
  } else {
    tran.successCount++;
  }

  if (tran.successCount > 1) {
    log->logReadSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
    trans_ht->erase(msg.transID);
  } 
  if (tran.failCount > 1) {
    log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
    trans_ht->erase(msg.transID);
  }
}


void MP2Node::handleCreateMsg(Message msg) {
  bool isCreated = createKeyValue(msg.key, msg.value, msg.replica);
  Message reply = Message(msg.transID, memberNode->addr, REPLY, isCreated); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());

  if (isCreated) {
    log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
  } else {
    log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
  }
}

void MP2Node::handleReadMsg(Message msg) {
  string value = readKey(msg.key);
  Message reply = Message(msg.transID, memberNode->addr, value); 

	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());

  if (value != "") {
    log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, value);
  } else {
    log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
  }
}

void MP2Node::handleUpdateMsg(Message msg) {
  bool isUpdated = updateKeyValue(msg.key, msg.value, msg.replica);
  Message reply = Message(msg.transID, memberNode->addr, REPLY, isUpdated); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());

  if (isUpdated) {
    log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
  } else {
    log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
  }
}

void MP2Node::handleDeleteMsg(Message msg) {
  bool isDeleted = deletekey(msg.key);
  Message reply = Message(msg.transID, memberNode->addr, REPLY, isDeleted); 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, reply.toString());

  if (isDeleted) {
    log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
  } else {
    log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
  }
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
  return;
}
