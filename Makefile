CXX ?= g++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -DNDEBUG
CPPFLAGS ?= -Iinclude -Isrc
LDFLAGS ?=
LDLIBS ?= -lpthread

SRC_DIR = src
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = bench

TARGET = $(BUILD_DIR)/liboxigraph-core.a
TEST_TARGET = $(BUILD_DIR)/unit_tests
BENCH_TARGET = $(BUILD_DIR)/benchmark

SRCS = $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst $(SRC_DIR)/%.cpp, $(BUILD_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS = $(patsubst $(TEST_DIR)/%.cpp, $(BUILD_DIR)/tests/%.o, $(TEST_SRCS))

BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.cpp)
BENCH_OBJS = $(patsubst $(BENCH_DIR)/%.cpp, $(BUILD_DIR)/bench/%.o, $(BENCH_SRCS))

.PHONY: all build test benchmark clean

all: build

build: $(BUILD_DIR) $(TARGET)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BUILD_DIR)/tests
	@mkdir -p $(BUILD_DIR)/bench

$(TARGET): $(OBJS)
	ar rcs $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@

test: $(BUILD_DIR) $(TEST_TARGET)
	@if [ -x $(TEST_TARGET) ]; then \
		./$(TEST_TARGET); \
	else \
		echo "No tests found or test target not built."; \
	fi

$(TEST_TARGET): $(TARGET) $(TEST_OBJS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(TEST_OBJS) $(TARGET) $(LDFLAGS) $(LDLIBS) -o $@

$(BUILD_DIR)/tests/%.o: $(TEST_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@

benchmark: $(BUILD_DIR) $(BENCH_TARGET)
	@if [ -x $(BENCH_TARGET) ]; then \
		./$(BENCH_TARGET); \
	else \
		echo "No benchmarks found or benchmark target not built."; \
	fi

$(BENCH_TARGET): $(TARGET) $(BENCH_OBJS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(BENCH_OBJS) $(TARGET) $(LDFLAGS) $(LDLIBS) -o $@

$(BUILD_DIR)/bench/%.o: $(BENCH_DIR)/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	@rm -rf $(BUILD_DIR)