#!/bin/sh
LD_LIBRARY_PATH=./node_modules/protobuf-for-node:../../protobuf/src/.libs  NODE_PATH=./node_modules/protobuf-for-node node debug tests.js
