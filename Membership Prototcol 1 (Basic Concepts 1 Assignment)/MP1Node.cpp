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
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
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
    MessageHdr *header = (MessageHdr *) data;
    if(header ->  msgType == JOINREQ) {
        // Extract fields from data message
        int *id = (int *) (data + sizeof(MessageHdr));
        short *port = (short *) (data + sizeof(MessageHdr) + sizeof(int));
        long *heartbeat = (long *) (data + sizeof(MessageHdr) + sizeof(int) + sizeof(short));

        // Add the node to local membership list if it doesn't already exist. Update if already present
        Address address;
        memset(&address, 0, sizeof(Address));
        *((int *)(&address.addr)) = *id;
        *(short *)(&address.addr[4]) = *port;
        bool found = false;
        for(int i = 0; i < memberNode -> memberList.size() && !found; i++) {
            if(memberNode -> memberList[i].getid() == *id && memberNode -> memberList[i].getport() == *port) {
                if(memberNode -> memberList[i].heartbeat < *heartbeat) {
                    memberNode -> memberList[i].setheartbeat(*heartbeat);
                    memberNode -> memberList[i].settimestamp(par->getcurrtime());
                }
                found = true;
            }
        }
        if(!found) {
            MemberListEntry memberListEntry = MemberListEntry(*id, *port, *heartbeat, par->getcurrtime());
            memberNode -> memberList.push_back(memberListEntry);
            log -> logNodeAdd(&(memberNode -> addr), &address);
        }

        // Generate a JOINREP message
        size_t msgSize = (sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + (memberNode->memberList.size() * sizeof(MemberListEntry))) * sizeof(char);
        void *msg = (MessageHdr *) malloc(msgSize);
        void * p = msg;
        ((MessageHdr *)msg)->msgType = JOINREP;
        msg = msg + sizeof(MessageHdr);
        memcpy((char *)(msg), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        msg = msg + sizeof(memberNode->addr.addr);
        for(int i = 0; i < memberNode->memberList.size(); i++) {
            printf("Coppying member %d\n", i);
            memcpy(msg, &(memberNode->memberList[i]), sizeof(MemberListEntry));
            msg = msg + sizeof(MemberListEntry);
        }
        emulNet->ENsend(&(memberNode->addr), &address, (char *) p, msgSize);
        // free(msg);
    } else if (header -> msgType == JOINREP) {
        // Membership request of the node has been accepted
        memberNode -> inGroup = true;
        printf("In message reply\n");
        // Extract peer information contained in the message
        data = data + sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
        if((size - sizeof(MessageHdr) - sizeof(memberNode->addr.addr)) > 0) {
            int i = (size - sizeof(MessageHdr) - sizeof(memberNode->addr.addr)) / sizeof(MemberListEntry);
            while(i--) {
                printf("scanning data");
                MemberListEntry *temp = (MemberListEntry *) malloc(sizeof(MemberListEntry));
                memcpy(temp, data, sizeof(MemberListEntry));
                Address address;
                memset(&address, 0, sizeof(Address));
                *((int *)(&address.addr)) = temp->id;
                *(short *)(&address.addr[4]) = temp->port;
                bool found = false;
                for(int i = 0; i < memberNode -> memberList.size() && !found; i++) {
                    if(memberNode -> memberList[i].getid() == temp->id && memberNode -> memberList[i].getport() == temp->port) {
                        if(memberNode -> memberList[i].heartbeat < temp->heartbeat) {
                            memberNode -> memberList[i].setheartbeat(temp->heartbeat);
                            memberNode -> memberList[i].settimestamp(par->getcurrtime());
                        }
                        found = true;
                    }
                }
                if(!found) {
                    MemberListEntry memberListEntry = MemberListEntry(temp->id, temp->port, temp->heartbeat, par->getcurrtime());
                    memberNode -> memberList.push_back(memberListEntry);
                    log -> logNodeAdd(&(memberNode -> addr), &address);
                }
                data = data + sizeof(MemberListEntry);
                free(temp);
            }
        }
    } else if(header -> msgType == PING) {
        // Extract fields from data message
        int *id = (int *) (data + sizeof(MessageHdr));
        short *port = (short *) (data + sizeof(MessageHdr) + sizeof(int));
        long *heartbeat = (long *) (data + sizeof(MessageHdr) + sizeof(int) + sizeof(short));

        // Add the node to local membership list if it doesn't already exist. Update if already present
        Address address;
        memset(&address, 0, sizeof(Address));
        *((int *)(&address.addr)) = *id;
        *(short *)(&address.addr[4]) = *port;
        bool found = false;
        for(int i = 0; i < memberNode -> memberList.size() && !found; i++) {
            if(memberNode -> memberList[i].getid() == *id && memberNode -> memberList[i].getport() == *port) {
                if(memberNode -> memberList[i].heartbeat < *heartbeat) {
                    memberNode -> memberList[i].setheartbeat(*heartbeat);
                    memberNode -> memberList[i].settimestamp(par->getcurrtime());
                }
                found = true;
            }
        }
        if(!found) {
            MemberListEntry memberListEntry = MemberListEntry(*id, *port, *heartbeat, par->getcurrtime());
            memberNode -> memberList.push_back(memberListEntry);
            log -> logNodeAdd(&(memberNode -> addr), &address);
        }
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // Increment node's heartbeat value
    memberNode->heartbeat = memberNode->heartbeat + 1;
    
    // Check if ping from some node has not been received
    vector<MemberListEntry>::iterator i = memberNode->memberList.begin();
    for(; i != memberNode -> memberList.end(); i++) {
        Address address;
        memset(&address, 0, sizeof(Address));
        *((int *)(&address.addr)) = i -> id;
        *(short *)(&address.addr[4]) = i -> port;
        if(par->getcurrtime() - i -> timestamp >= TREMOVE) {
            memberNode->memberList.erase(i);
            log->logNodeRemove(&(memberNode -> addr), &address);
        }
    }

    // Send a PING message to all other nodes in the group
    for(int i = 0; i < memberNode -> memberList.size(); i++) {
        Address address;
        memset(&address, 0, sizeof(Address));
        *((int *)(&address.addr)) = memberNode -> memberList[i]. id;
        *(short *)(&address.addr[4]) = memberNode -> memberList[i].port;
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1;
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = PING;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        emulNet->ENsend(&(memberNode->addr), &address, (char *) msg, msgsize);
        free(msg);
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
