#!/bin/sh
protoc --descriptor_set_out=mvstore.desc --include_imports --proto_path=../../kernel/src/ ../../kernel/src/mvstore.proto
