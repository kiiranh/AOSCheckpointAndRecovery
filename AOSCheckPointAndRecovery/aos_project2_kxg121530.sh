#!/bin/bash

echo "Checking source directory...";
if [ -d "src" ]; then
    echo "Source directory exists. Compiling...";
    
    if [ -d "bin" ]; then
	rm -rf bin/*;
    else 
	mkdir "bin";
    fi

    # Files need to compiled in specific order
    javac -cp src src/model/Message.java -d bin
    javac -cp src src/model/Connection.java -d bin
    javac -cp src src/model/AppState.java -d bin
    javac -cp src src/common/Backoff.java -d bin
    javac -cp src src/common/Utility.java -d bin
    javac -cp src src/common/Test.java -d bin
    javac -cp src src/application/DistributedApplication.java -d bin
    javac -cp src src/service/KooToueg.java -d bin
    javac -cp src src/startup/Node.java -d bin
 
    echo "Compiling source done. Binaries placed in \"bin\" directory";

else
    echo "\"src\" directory does not exist.";

fi
