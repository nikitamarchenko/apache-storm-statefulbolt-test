#!/bin/bash
set -x

mvn package
cd target
cp apache-storm-statefulbolt-test-1.0-SNAPSHOT.jar topology.jar

docker run -it --rm \
-v$(pwd)/topology.jar:/topology.jar \
-v$(pwd)/../src/main/resources/storm-siem-topology.config.properties:/storm-siem-topology.config.properties \
--network apachestormstatefulbolttest_default \
storm \
storm jar /topology.jar name.nmarchenko.org.apache.storm.statefulbolttest.StormGenericTopology /storm-siem-topology.config.properties
cd -