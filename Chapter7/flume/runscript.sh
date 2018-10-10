#!/bin/bash
cd $FLUME_HOME
bin/flume-ng agent --conf conf --conf-file example.conf --name myagent -Dflume.root.logger=INFO,console
# Run Telnet
telnet localhost 9999