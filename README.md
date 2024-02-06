# chord_dht

This repository contains an example implementation of the Chord Distributed Hash Table (DHT) protocol.
This implementation is not meant to be for productive use. Instead, it was created just for personal education.

### About the Chord Protocol

The Chord protocol is a decentralized lookup service that efficiently locates the node responsible for storing a particular piece of data within a distributed network. 

The Chord protocol is a distributed hash table (DHT) framework that organizes network nodes in a ring topology to facilitate efficient data location and retrieval in peer-to-peer networks. 
Each node and data item is assigned a unique identifier using a consistent hashing mechanism.

Each node maintains links to a logarithmically scaled subset of other nodes on the ring. 
This structure allows any node to reach any other node on the ring in a logarithmic number of steps.
Therefore, each node is able to quickly find the responsible node for a given data item.

For an in-depth understanding, we recommend reading the original Chord paper.

### Design Decisions

I have decided to put the implementation of the chord protocol in its own library. For extern users
this library mainly provides the following functions:
- Network creation
- Node join and leave operations
- Key-based node lookup

A separate data layer must be developed on top of this Chord library for practical applications, as illustrated in our demo project. 

To facilitate communication between the Chord and data layers (necessary for operations like data transfer) a notification system has been implemented. 
This system enables the Chord layer to inform the data layer about events (e.g. data transfer), allowing for efficient data handling and processing.

### How to Use

We have provided a simple demo to showcase the functionality of the Chord DHT implementation. 
Here is how to get it running:

1. Build the Project: Ensure Docker and Docker Compose are installed on your system. 
   Then, build the project using Docker Compose to set up the environment and dependencies.

2. Run the Demo: Execute the demo using Docker Compose. 
   The demo simulates a small network where nodes join, store, and retrieve data. Specifically, each node will:
   - Join the network.
   - Store three key-value pairs.
   - Retrieve the three stored values after a short delay.

The demo concludes automatically after approximately 20 seconds.