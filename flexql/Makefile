CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Iinclude
LDFLAGS  := -lpthread

BINDIR := bin
OBJDIR := obj

COMMON_SRC := src/network/socket_utils.cpp \
              src/parser/sql_parser.cpp \
              src/storage/database_engine.cpp \
              src/storage/wal_writer.cpp

COMMON_OBJ := $(patsubst src/%.cpp,$(OBJDIR)/%.o,$(COMMON_SRC))

API_SRC  := src/client/flexql_api.cpp
API_OBJ  := $(OBJDIR)/client/flexql_api.o

SERVER_SRC := src/server/server_main.cpp src/server/server.cpp
SERVER_OBJ := $(patsubst src/%.cpp,$(OBJDIR)/%.o,$(SERVER_SRC))

CLIENT_MAIN_OBJ := $(OBJDIR)/client/client_main.o
SMOKE_OBJ       := $(OBJDIR)/tests/smoke.o
FUNC_OBJ        := $(OBJDIR)/tests/functional_test.o

LIB := $(OBJDIR)/libflexql_client_api.a

.PHONY: all clean

all: $(BINDIR)/flexql-server \
     $(BINDIR)/flexql-client \
     $(BINDIR)/flexql-smoke \
     $(BINDIR)/flexql-functional-test

$(LIB): $(COMMON_OBJ) $(API_OBJ)
	@mkdir -p $(dir $@)
	ar rcs $@ $^

$(BINDIR)/flexql-server: $(SERVER_OBJ) $(COMMON_OBJ)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(BINDIR)/flexql-client: $(CLIENT_MAIN_OBJ) $(LIB)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(BINDIR)/flexql-smoke: $(SMOKE_OBJ) $(LIB)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(BINDIR)/flexql-functional-test: $(FUNC_OBJ) $(LIB)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(OBJDIR)/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJDIR)/tests/%.o: tests/%.cpp
	@mkdir -p $(OBJDIR)/tests
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -rf $(OBJDIR) $(BINDIR)
