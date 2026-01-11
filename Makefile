CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I include

SRCS = src/catalog/catalog.cpp \
       src/parser/sql_parser.cpp \
       src/executor/ddl_executor.cpp \
       src/executor/dml_executor.cpp \
       src/executor/select_executor.cpp \
       src/executor/aggregate_executor.cpp \
       src/executor/join_executor.cpp \
       src/nanodb.cpp \
       main.cpp

TARGET = nanodb

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)

.PHONY: all clean
