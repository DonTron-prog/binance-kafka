#!/bin/bash

# Compile and run the RawTradeConsumer
echo "Building the project..."
mvn clean compile

echo ""
echo "Starting Raw Trade Consumer..."
echo "==============================="
mvn exec:java -Dexec.mainClass="com.crypto.cli.RawTradeConsumer"