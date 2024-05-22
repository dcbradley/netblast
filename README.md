NetBlast, not to be confused with NetBLAST, is a collection of Python
scripts for testing network bandwidth.  It is particularly suited to
tests involving many computers simultaneously transferring data.

# Requirements

Python 3.9.  Only known to work in Linux.

# Basic Usage

Run netblast-manager.py on a computer and port that all the other
participating computers can access.  Use the --port option or let it
pick a random port.

    netblast-manager.py --port 10000 >& netblast.log

Capture the output to a file.  This will be used for analysis later.

Run netblast-worker.py on all participating computers.  The --manager
option must be used to tell it the hostname/IP and port of the
manager.

    netblast-worker.py --manager example.host.net:10000

The test will run for a finite time (default 2 minutes).  At any time,
the test can be ended by killing the manager.  Analyze the log file
using netblast-analyze.py.  It will produce a CSV file showing the
total volume of flow and number of participating computers over time.
The analysis is typically run after the test finishes, but it could
be run periodically during the test to see how things are going.

    netblast-analyze.py netblast.log netblast.csv

# Options

The manager's command-line options control how long the test runs,
which direction data flows between which computers, and whether to
slowly ramp up the number of active flows.

The worker's command-line options control the network port used by the
worker and whether to run in the background.

Workers can act as servers, clients, or both (the default).  For a
network flow to happen between two workers, at least one of them
(acting as the client) must be able to connect to the other one's
network port (the server).  The direction of flow is from the client
to the server by default, but the manager can be configured to reverse
the direction or to transfer in both directions.

An example configuration that specifies that workers in two specific
subnets should act as clients and all others as servers, with data
flowing from servers to clients, adding one more flow every 5 seconds:

    netblast-manager.py --port 10000 --direction r --clients 10.1.2.0/24 --clients 10.2.2.0/25 --ramp-delay 5

The analyzer's command-line options control which transfers are
included in the analysis and how big the reporting time steps are.

Example that analyzes transfers to a specific subnet:

    netblast-analyze.py --dest 10.1.2.0/24 netblast.log netblast.csv

# Author

NetBlast was written by Dan Bradley <dan@physics.wisc.edu> to conduct
bandwidth tests between large computer clusters.
