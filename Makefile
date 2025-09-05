#####################################################
# RCS INFORMATION:
#   $RCSfile: Makefile,v $
#   $Revision: 1.2 $
#   $Author: choudhury $
#   $Locker:  $
#   $Date: 2008/06/10 20:17:29 $
#   $State: Exp $
#####################################################

##
##   Usage:
##
##   make            - make compile&link executable into bin/pbcdl_comm
##   make clean      - remove ./obj/ & ./bin/ files
##   make install    - copy pbcdl_comm executable from $(OUT_DIR), eg: ./bin
##                     to operational bin directory $(OP_BIN_DIR), eg: ../bin/
##
OBJ_DIR  = ./obj
SRC_DIR  = src
C_SRCS   = $(shell ls $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(C_SRCS))
CPP_SRCS = $(shell ls $(SRC_DIR)/*.cpp)
OBJS += $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(CPP_SRCS))

-include $(OBJS:.o=.d)
CXXFLAGS    = -O0 -g -c -pedantic -Wall `xml2-config --cflags` --std=c++03
XMLLFLAGS = `xml2-config --libs` 

##############################################################################
# Edit to modify output options 
##############################################################################
CXX       = g++
OUT_DIR = ./bin
OP_BIN_DIR = /usr/local/bin
BIN_NAME  = pbcdl_comm
CXXFLAGS += $(shell pkg-config --cflags log4cpp libxml-2.0) -MMD -MP
LDLIBS   = $(shell pkg-config --libs log4cpp libxml-2.0) -lpthread
LDFLAGS	 = -rdynamic


TARGET = $(OUT_DIR)/$(BIN_NAME)

# link
$(BIN_NAME): $(OBJS)
	@mkdir -p $(OUT_DIR)
	$(CXX) $(LDFLAGS) -o $(OUT_DIR)/$@ $(OBJS) $(LDLIBS) $(XMLLFLAGS)

# generic compile rule
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

all: $(BIN_NAME)

clean  : 
	rm -f $(TARGET)
	rm -f $(OBJS)

install:
	@echo "make install: copying $(TARGET) to $(OP_BIN_DIR)"
	@mkdir -p $(OP_BIN_DIR)
	@/bin/cp -p $(TARGET) $(OP_BIN_DIR)


