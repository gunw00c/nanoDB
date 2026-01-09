CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I include

SRCS = src/storage/disk_manager.cpp \
       src/catalog/catalog.cpp \
       src/parser/sql_parser.cpp \
       src/nanodb.cpp \
       main.cpp

TARGET = nanodb

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)

.PHONY: all clean
