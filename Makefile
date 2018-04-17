.PHONY: default test clean

TARGET = VolumeTest
SRCS = Volume.cxx VolumeTest.cxx

LIBS = -L/usr/local/lib -lgtest

OBJS = $(SRCS:.cxx=.o)
DEPS = $(OBJS:.o=.d)

CXX = g++
CXXFLAGS = -g -MMD -Wall

default: $(TARGET)

-include $(DEPS)

$(TARGET): $(OBJS)
	$(CXX)  -o $@  $(OBJS) $(LIBS)

%.o: %.cxx
	$(CXX)  -o $@ -c $<  $(CXXFLAGS)

clean:
	rm -rf $(TARGET) $(OBJS) $(DEPS)
	
test: default
	./$(TARGET)

