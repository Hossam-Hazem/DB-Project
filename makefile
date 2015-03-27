JFLAGS = -g
JC = javac
SRCPATH = ./src/CodeVision
CLSSPATH = ./classes/CodeVision

libraries: 
		   $(JC) -d $(CLSSPATH) -classpath $(CLSSPATH) $(SRCPATH)*.java

all: libraries


clean: 
	   rm $(CLSSPATH)*

