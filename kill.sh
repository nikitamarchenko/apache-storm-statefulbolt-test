#!/bin/bash
set -x

docker run -it --rm \
--network apachestormstatefulbolttest_default \
storm \
storm kill topologyName 
