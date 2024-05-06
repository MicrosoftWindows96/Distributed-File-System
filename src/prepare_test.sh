#!/bin/bash

# Set the path to your source directory
src_path="/Users/zagrosi/IdeaProjects/COMP2207---Distributed-Systems/src"

# Set the path to the directory where you want to run the tests
test_dir="/Users/zagrosi/IdeaProjects/COMP2207---Distributed-Systems/test"

# Create the test directory if it doesn't exist
mkdir -p "$test_dir"

# Copy the necessary files to the test directory
cp "$src_path/Controller.java" "$test_dir"
cp "$src_path/Dstore.java" "$test_dir"
cp "$src_path/ClientMain.java" "$test_dir"
cp "$src_path/Protocol.java" "$test_dir"
cp "$src_path/client.jar" "$test_dir"
cp "$src_path/my_policy.policy" "$test_dir"
cp "$src_path/validate_submission.sh" "$test_dir"

# Change to the test directory
cd "$test_dir"

# Make the validation script executable
chmod +x validate_submission.sh

# Create a zip file containing the required files
zip submission.zip Controller.java Dstore.java Protocol.java

# Define the ports
cport=12345
dport1=12346
dport2=12347
dport3=12348

# Kill the processes that are using the ports
for port in $cport $dport1 $dport2 $dport3
do
  pid=$(lsof -t -i:$port)
  if [ -n "$pid" ]; then
    kill -9 $pid
    echo "Killed process $pid that was using port $port"
  fi
done

# Run the validation script with the zip file and a wait time of 10 seconds
./validate_submission.sh submission.zip 10