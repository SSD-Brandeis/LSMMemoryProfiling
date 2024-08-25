CXX=clang++
CXXFLAGS=-g -fsanitize=fuzzer,address -I$(PB_OUT_DIR)

LPM_DIR=../vendor/libprotobuf-mutator
PROTOBUF_DIR=$(LPM_DIR)/build/external.protobuf

PROTOBUF_LIB=$(PROTOBUF_DIR)/lib/libprotobufd.a
LPM_LIB=$(LPM_DIR)/build/src/libfuzzer/libprotobuf-mutator-libfuzzer.a $(LPM_DIR)/build/src/libprotobuf-mutator.a
INC=-I$(PROTOBUF_DIR)/include -I$(LPM_DIR)
DFUZZ=-DLLVMFuzzerTestOneInput=FuzzTEST

PB_SRC_DIR=./proto
PB_OUT_DIR=./proto/gen/
PB_SRC=$(PB_SRC_DIR)/*.proto
PROTOC=$(PROTOBUF_DIR)/bin/protoc

.PHONY: proto clean

proto:
	mkdir -p $(PB_SRC_DIR)
	$(PROTOC) --proto_path=$(PB_SRC_DIR) --cpp_out=$(PB_OUT_DIR) $(PB_SRC_DIR)/*.proto

trie_fuzzer: proto trie_fuzzer.cc $(PB_OUT_DIR)/db_operation.pb.cc
	$(CXX) $(CXXFLAGS) -o $@ trie_fuzzer.cc $(PB_OUT_DIR)/db_operation.pb.cc  $(LPM_LIB) $(PROTOBUF_LIB) $(INC)

clean:
	rm -rf trie_fuzzer proto/gen/*

all: trie_fuzzer