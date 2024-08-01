# A membership protocol

This MP (Machine Programming assignment) is about implementing a membership protocol similar to one
we discussed in class. Since it is infeasible to run a thousand cluster nodes (peers) over a real network, we 
are providing you with an implementation of an emulated network layer (EmulNet). Your membership 
protocol implementation will sit above EmulNet in a peer- to-peer (P2P) layer, but below an App layer.
Think of this like a three-layer protocol stack with Application, P2P, and EmulNet as the three layers (from
top to bottom). More details are below.

# Requirements

Your protocol must satisfy:

i) Completeness all the time: every non-faulty process must detect every node join, failure, and leave, and
ii) Accuracy of failure detection when there are no message losses and message delays are small. When there are message losses

Completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.
