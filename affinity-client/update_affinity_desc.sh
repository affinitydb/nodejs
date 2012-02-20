#!/bin/sh
protoc --descriptor_set_out=affinity.desc --include_imports --proto_path=../../kernel/src/ ../../kernel/src/affinity.proto
