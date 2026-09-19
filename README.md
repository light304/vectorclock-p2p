Vector Clock P2P Messaging System

Overview

A small peer-to-peer chat system. Each peer is its own Java process and talks directly to every other peer over TCP. There is no central server. Messages carry vector clocks, and each peer holds back any message that arrived before the messages that causally came first, so delivery order always respects cause and effect. Peers also send heartbeats and mark a peer as suspected if it goes quiet. There are no external dependencies.

Requirements

JDK 17 or newer. Maven is optional.

Build with Maven (default)

"mvn compile"

Classes are written to target/classes.

Build with plain javac

"mkdir out"

"javac -d out src/main/java/p2p/*.java"

Classes are written to out.

Run the tests

Each test is a standalone class that prints PASS or FAIL. Swap "target/classes" for "out" if you built with javac.

"java -cp target/classes p2p.VectorClockTest"

"java -cp target/classes p2p.MessageTest"

"java -cp target/classes p2p.DeliveryManagerTest"

"java -cp target/classes p2p.NetworkingSmokeTest"

"java -cp target/classes p2p.FailureDetectorTest"

"java -cp target/classes p2p.OutboundRouterTest"

"java -cp target/classes p2p.CliTest"

Run the peers

Open three terminals in the project root and start one peer in each. Peers are defined in peers.json. To add more, add another entry and start it the same way.

Terminal 1:

"java -cp target/classes p2p.Peer peers.json peer-A"

Terminal 2:

"java -cp target/classes p2p.Peer peers.json peer-B"

Terminal 3:

"java -cp target/classes p2p.Peer peers.json peer-C"

If you built with javac, use "java -cp out p2p.Peer peers.json peer-A" (and so on for B and C).

Commands

Type these into any running peer:

"send <text>" broadcasts a message to all peers.

"send @<peerId> <text>" sends to one peer.

"clock" shows this peer's vector clock.

"peers" shows each peer as ALIVE, SUSPECTED or UNKNOWN.

"queue" shows messages held back waiting on earlier ones.

"history" shows recent send and deliver activity.

"delay <peerId> <seconds>" delays outgoing messages to one peer (0 clears it).

"heartbeat" shows heartbeats received since last checked.

"heartbeat -display" shows heartbeats live. "heartbeat -hide" turns that off.

"crash" simulates a crash with no goodbye message.

"quit" shuts the peer down.

"help" lists the commands.
