/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

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
	id = *(int*)(&memberNode->addr.addr);
	port = *(short*)(&memberNode->addr.addr[4]);
  numberOfRandomTarget = 4;

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 100;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = 2 * TFAIL;
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
  free(memberNode);
  return 0;
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
  MessageHdr * msgHdr = (MessageHdr *) malloc(sizeof(MessageHdr));

  memcpy(msgHdr, data, sizeof(MessageHdr));

  // Handle JOINREQ type message
  if (msgHdr->msgType == JOINREQ) {
    MessageJOINREQ joinReqMsg;

    memcpy(&joinReqMsg.id, data+sizeof(MessageHdr), sizeof(int));
    memcpy(&joinReqMsg.port, data+sizeof(MessageHdr)+sizeof(int), sizeof(short));
    memcpy(&joinReqMsg.heartbeat, data+sizeof(MessageHdr)+sizeof(int)+sizeof(short)+1, sizeof(long));
    
    // std::cout << "Test: " << joinReqMsg.id  << ":" << joinReqMsg.port << " " << joinReqMsg.heartbeat << std::endl;
    handleJOINREQ(&joinReqMsg);

  }

  // Handle JOINREP or GOSSIP type message
  if (msgHdr->msgType == JOINREP || msgHDR->msgType == GOSSIP) {
    MessageJOINREP msg;
    memcpy(&msg.id, data+sizeof(MessageHdr), sizeof(int));
    memcpy(&msg.port, data+sizeof(MessageHdr)+sizeof(int), sizeof(short));
    memcpy(&msg.numberOfMember, data+sizeof(MessageHdr)+sizeof(int)+sizeof(short), sizeof(int));
    msg.memberList = (MemberListEntry *) malloc(sizeof(MemberListEntry)*msg.numberOfMember);
    memcpy(msg.memberList, data+sizeof(MessageHdr)+sizeof(int)+sizeof(short)+sizeof(int), sizeof(MemberListEntry)*msg.numberOfMember);
    
    // Handle JOINREP type message
    if (msgHdr->msgType == JOINREP) {
      handleJOINREP(&msg);
    }
    // Handle GOSSIP type message
    if (msgHdr->msgType == GOSSIP) {
      handleGOSSIP(&msg);
    }

    //std::cout << "Test: " << msg.id  << ":" << msg.port << " " << msg.numberOfMember << std::endl;
    // for(int i=0;i<msg.numberOfMember; i++){
    // std::cout << msg.memberList[i].id << std::endl;
    // }

    free(msg.memberList);
  }

  free(msgHdr);

  return true;
}

void MP1Node::handleGOSSIP(MessageJOINREQ * msg) {

  for(int i=0; i < msg->numberOfMember; i++){
    bool isExist = false;
    MessageListEntry & e = msg->memberList[i];

    for(int j=0; j < memberNode->memberList.size(); j++){
      if (e.id == memberNode->memberList[j].id && e.port == memberNode->memberList[j].port) {
        isExist = true;

        if (e.heartbeat != memberNode->memberList[j].heartbeat && (long)(memberNode->memberList[j].timestamp + par->pingCounter) >= par->getcurrtime()) {
          memberNode->memberList[j].heartbeat = e.heartbeat;   
          memberNode->memberList[j].timestamp = (long) par->getcurrtime();   
        }
        break;
      }
    }
    // Add non exist entries into memberList
    if (!isExist) {
      e.timestamp = (long) par->getcurrtime();
      memberNode->memberList.push_back(e);
      Address addr;
      memcpy(&addr.addr, &e.id, sizeof(int));
      memcpy(&addr.addr[4], &e.port, sizeof(short));
      memberNode->nnb++;
      log->logNodeAdd(&memberNode->addr, &addr);
    }

    if (e.id == memberNode->id && e.port == memberNode->port) {
      memberNode->inGroup = true;
      continue;
    }
  }

  return;
}

void MP1Node::handleJOINREP(MessageJOINREP * msg) {

  // Add non exist entries into memberList
  // mark itself in the group
  for(int i=0; i < msg->numberOfMember; i++){
    bool isExist = false;
    for(int j=0; j < memberNode->memberList.size(); j++){
      if (msg->memberList[i].id == memberNode->memberList[j].id && msg->memberList[i].port == memberNode->memberList[j].port) {
        isExist = true;
        break;
      }
    }
    if (!isExist) {
      msg->memberList[i].timestamp = (long) par->getcurrtime();
      memberNode->memberList.push_back(msg->memberList[i]);
      Address addr;
      memcpy(&addr.addr, &msg->memberList[i].id, sizeof(int));
      memcpy(&addr.addr[4], &msg->memberList[i].port, sizeof(short));
      memberNode->nnb++;
      log->logNodeAdd(&memberNode->addr, &addr);
    }

    if (msg->memberList[i].id == memberNode->id && msg->memberList[i].port == memberNode->port) {
      memberNode->inGroup = true;
      continue;
    }
  }
  
  return;
}

void MP1Node::handleJOINREQ(MessageJOINREQ * msg) {
  bool inMemberList = false;

  Address joinAddr;
  memcpy(&joinAddr.addr, &msg->id, sizeof(int));
  memcpy(&joinAddr.addr[4], &msg->port, sizeof(short));

  for(const MemberListEntry& e: memberNode->memberList) {
    if (e.id == msg->id && e.port == msg->port) {
      inMemberList = true;
      break;
    }
  }

  // Add joiner to its member list if joiner is not in the list
  if (!inMemberList) {
    // Add the entry
    MemberListEntry * entry = new MemberListEntry(msg->id, msg->port, msg->heartbeat, (long) par->getcurrtime());
    memberNode->memberList.push_back(*entry);
    // Increase nnb
    memberNode->nnb++;

    // printf("%d\n", entry->heartbeat);

    // Log Node Add
    log->logNodeAdd(&memberNode->addr, &joinAddr);
  }

  // Send the JOINREP back to joiner
  MemberListEntry * memberListArray = memberNode->memberList.data();
  int memberListSize = memberNode->memberList.size();
  size_t replysize = sizeof(MessageHdr) + sizeof(int) + sizeof(short) + sizeof(int) + sizeof(MemberListEntry)*memberListSize;
  MessageHdr * reply = (MessageHdr *) malloc(replysize * sizeof(char));
  reply->msgType = JOINREP;
  memcpy((char *)(reply+1), &id, sizeof(int));
  memcpy((char *)(reply+1)+sizeof(int), &port, sizeof(short));
  memcpy((char *)(reply+1)+sizeof(int)+sizeof(short), &memberListSize, sizeof(int));
  memcpy((char *)(reply+1)+sizeof(int)+sizeof(short)+sizeof(int), memberListArray, sizeof(MemberListEntry)*memberListSize);

  emulNet->ENsend(&memberNode->addr, &joinAddr, (char *)reply, replysize);

  free(reply);

  return;
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
 
  bool inMemberList = false;
  vector<MemberListEntry> newMemberList;

  // Increase its heartbeat  
  memberNode->heartbeat++;

  // Update new membership list
  for(MemberListEntry * e: memberNode->memberList) {
    if (e.id == msg->id && e.port == msg->port) {
      inMemberList = true;
      e.heartbeat = memberNode->heartbeat; 
      e.timestamp = (long) par->getcurrtime(); 
    }
    // Remove timeouted entry 
    if ( (long) (e.timestamp + memberNode->timeOutCounter) < par->getcurrtime() ) {
      Address address;
      memcpy(&address->addr[0], &e.id, sizeof(int));
      memcpy(&address->addr[4], &e.port, sizeof(short));
      if(!isNullAddress(&address)) {
        memberNode->nnb--;
        log->logNodeRemove(&memberNode->addr, &address); 
      }
      continue;
    }
    newMemberList.push_back(e);
  }

  // Add itself into the member list if it is not in the group
  if (!inMemberList) {
    MemberListEntry * e = new MemberListEntry(id, port, memberNOde->heartbeat, par->getcurrtime());
    newMemberList.push_back(e);
  }

  // memberList = new member list
  memberNode->memberList = newMemberList;
  free(newMemberList);

  // Get random targets
  int n = (int) min(memberNode->nnb, memberNode->numberOfRandomTarget);
  Address * randAddrs = (Address *) malloc(sizeof(Address)*n);
  genRandomAddr(id, port, &memberNode, randAddrs, n);

  // Send GOSSIP msg to selected random targets
  for (int i =0; i < n; i++) {
    MemberListEntry * memberListArray = memberNode->memberList.data();
    int memberListSize = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(int) + sizeof(short) + sizeof(int) + sizeof(MemberListEntry)*memberListSize;
    MessageHdr * msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = GOSSIP;
    memcpy((char *)(msg+1), &id, sizeof(int));
    memcpy((char *)(msg+1)+sizeof(int), &port, sizeof(short));
    memcpy((char *)(msg+1)+sizeof(int)+sizeof(short), &memberListSize, sizeof(int));
    memcpy((char *)(msg+1)+sizeof(int)+sizeof(short)+sizeof(int), memberListArray, sizeof(MemberListEntry)*memberListSize);

    emulNet->ENsend(&memberNode->addr, &randAddrs[i], (char *)msg, msgsize);
  }

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

void MP1Node::genRandomAddr(int id, short port, Member *memberNode, Address *address, int n) {
  bitset<512> bitmap = 0; 
  int i;
  char addr[6];
  int count = 0;

  while(count < n) {
    i = rand() % memberNode->memberList.size();
    if (bitmap[i] != true && (memberNode->memberList[i].id != id || memberNode->memberList[i].port != port)) {
      bitmap[i] = true;
      
      memcpy(&addr[0], &memberNode->memberList[i].id, sizeof(int));
      memcpy(&addr[4], &memberNode->memberList[i].port, sizeof(short));
      memcpy(&address[count].addr, &addr, sizeof(char)*6);
      count++;
    }
  }
}
