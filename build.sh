#!/bin/sh
#mvn clean  package assembly:assembly  dependency:copy-dependencies -Dmaven.test.skip=true

mvn  clean  package install  -Dmaven.test.skip=true
