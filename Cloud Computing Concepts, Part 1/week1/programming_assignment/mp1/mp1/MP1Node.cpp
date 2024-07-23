/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <cstdlib>
#include <bitset>
 

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = TREMOVE;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

  MessageHdr * msg = (MessageHdr *) malloc(sizeof(MessageHdr));
  Address * addr = (Address *) malloc(sizeof(Address));
  string address;

  memcpy(msg, data, sizeof(MessageHdr));
  memcpy(addr->addr, data + sizeof(MessageHdr), sizeof(addr->addr));

  address = addr->getAddress();

  // Handle JOINREQ type message
  if (msg->msgType == JOINREQ) {
    // parse the message
    long heartbeat;
		size_t pos = address.find(":");
		int id = stoi(address.substr(0, pos));
		short port = (short)stoi(address.substr(pos + 1, address.size()-pos-1));

    memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(addr->addr), sizeof(long));

    // add an new entry to the member list
    MemberListEntry * entry = new MemberListEntry(id, port, heartbeat, (long) par->getcurrtime());
    memberNode->memberList.push_back(*entry);
    log->logNodeAdd(&memberNode->addr, addr);
    // memberNodeMap[addr->getAddress()] = true;
    memberNode->nnb = (int) memberNode->memberList.size() - 1;

    // create joinrep message
    size_t replyMsgSize = sizeof(MessageHdr) + sizeof(&memberNode->addr.addr) + sizeof(int) + sizeof(MemberListEntry)*(memberNode->nnb + 1);
    MessageHdr * replyMsg = (MessageHdr *) malloc(replyMsgSize * sizeof(char)); 
    replyMsg->msgType = JOINREP;
    memcpy((char *)(replyMsg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(replyMsg+1) + sizeof(memberNode->addr.addr), &memberNode->nnb+1, sizeof(int));
    memcpy((char *)(replyMsg+1) + sizeof(memberNode->addr.addr) + sizeof(int), memberNode->memberList.data(), sizeof(MemberListEntry)*(memberNode->nnb+1));

    // send JOINREP message to join requestor 
    emulNet->ENsend(&memberNode->addr, addr, (char *)replyMsg, replyMsgSize);

    free(replyMsg);
  }

  // Handle JOINREP type message
  if (msg->msgType == JOINREP) {
    memcpy(&memberNode->nnb, data + sizeof(MessageHdr) + sizeof(addr->addr), sizeof(int));

    MemberListEntry * entry = (MemberListEntry *) malloc(sizeof(MemberListEntry)*(memberNode->nnb+1));

    memcpy(entry, data + sizeof(MessageHdr) + sizeof(addr->addr) + sizeof(int), sizeof(MemberListEntry)*(memberNode->nnb+1));

    // Add entries to its member list where the timestamp is its current time
    memberNode->memberList.clear();
    for(int i=0; i < memberNode->nnb; i++){
      entry[i].timestamp = par->getcurrtime();
      memberNode->memberList.push_back(entry[i]);
      log->logNodeAdd(&memberNode->addr, addr);
    }
    
    if (!memberNode->inGroup) {
      memberNode->inGroup = true;
    }
  }

  // Handle GOSSIP type message
  if (msg->msgType == GOSSIP) {
    int listSize;
    memcpy(&listSize, data + sizeof(MessageHdr) + sizeof(addr->addr), sizeof(int));

    MemberListEntry * entry = (MemberListEntry *) malloc(sizeof(MemberListEntry)*listSize);

    memcpy(entry, data + sizeof(MessageHdr) + sizeof(addr->addr) + sizeof(int), sizeof(MemberListEntry)*listSize);

    int currtime = par->getcurrtime();

    vector<MemberListEntry>& memberList = memberNode->memberList;

    for(int i=0; i < listSize; i++) {
      bool isExist = false;
      for(int j=0; j< memberList.size(); j++) {
        if (memberList[j].id == entry[i].id && entry[i].port == entry[i].port){
          isExist = true; 
          if (memberList[j].heartbeat < entry[j].heartbeat && memberList[j].timestamp+memberNode->pingCounter >= currtime) {
            memberList[j].heartbeat = entry[j].heartbeat; 
            memberList[j].timestamp = currtime;
          }
        }
      }
      if (!isExist) {
        entry[i].timestamp = currtime;
        memberList.push_back(entry[i]);
        Address address;
        memcpy(&address.addr[0], &entry[i].id, sizeof(int));
        memcpy(&address.addr[4], &entry[i].port, sizeof(short));
        log->logNodeAdd(&memberNode->addr, &address);
      }
    }

  }

  free(msg);
  free(addr);

  return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    vector<MemberListEntry> newMemberList;

    int myId;
    short myPort;
    char addr[6];
		memcpy(&myId, &memberNode->addr.addr[0], sizeof(int));
		memcpy(&myPort, &memberNode->addr.addr[4], sizeof(short));
    int currtime = par->getcurrtime();

    // Generate new member list
    for(int i=0; i < memberNode->nnb+1; i++){
      // Ingore its entry
      if( memberNode->memberList[i].id == myId && memberNode->memberList[i].port == myPort ) {
        continue;
      }
      // Not add timeouted entry into new member list
      if ( memberNode->memberList[i].timestamp + memberNode->timeOutCounter < currtime ) {
        Address address;
        memcpy(&addr[0], &memberNode->memberList[i].id, sizeof(int));
        memcpy(&addr[4], &memberNode->memberList[i].port, sizeof(short));
        memcpy(&address.addr, &addr, sizeof(char)*6);
        if(!isNullAddress(&address)) {
          log->logNodeRemove(&memberNode->addr, &address); 
        }
        continue;
      }
      newMemberList.push_back(memberNode->memberList[i]);
    }

    // Update its membership list
    memberNode->memberList = newMemberList;
    memberNode->nnb = newMemberList.size();

    // Add itself to membership list
    MemberListEntry * entry = new MemberListEntry(myId, myPort, ++(memberNode->heartbeat), currtime);
    memberNode->memberList.push_back(*entry);

    // Propagate its membership list via GOSSIP message to random 2 * log(N) members
    size_t msgSize = sizeof(MessageHdr) + sizeof(&memberNode->addr.addr) + sizeof(int) + sizeof(MemberListEntry)*(memberNode->nnb+1);
    MessageHdr * msg = (MessageHdr *) malloc(msgSize * sizeof(char)); 
    msg->msgType = GOSSIP;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->nnb, sizeof(int));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) + sizeof(int), memberNode->memberList.data(), sizeof(MemberListEntry)*(memberNode->nnb+1));

    // Generate random members
    int numberOfTargets = memberNode->nnb;
    Address * address = (Address *) malloc(numberOfTargets * sizeof(Address));
    genRandomAddr(myId, myPort, memberNode, address, numberOfTargets);

    for (int i=0; i < numberOfTargets; i++) {
      // send GOSSIP message to to random member
      emulNet->ENsend(&memberNode->addr, &address[i], (char *)msg, msgSize);
    }

    free(msg);
    free(address);

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::genRandomAddr(int id, short port, Member *memberNode, Address * address, int n) {
  bitset<512> bitmap = 0; 
  int i;
  char addr[6];
  int count = 0;

  while(count < n) {
    i = rand() % memberNode->memberList.size();
    if (bitmap[i] != true && (memberNode->memberList[i].id != id||memberNode->memberList[i].port != port)) {
      bitmap[i] = true;
      
      memcpy(&addr[0], &memberNode->memberList[i].id, sizeof(int));
      memcpy(&addr[4], &memberNode->memberList[i].port, sizeof(short));
      memcpy(&address[count].addr, &addr, sizeof(char)*6);
      count++;
    }
  }
}
