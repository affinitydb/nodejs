#!/bin/sh
LD_LIBRARY_PATH=./node_modules/protobuf-for-node:../../protobuf/lib  NODE_PATH=./node_modules/protobuf-for-node node tests.js
