#!/bin/bash

# Script to run the filtered trade consumer

SYMBOL=${1:-BTC}

echo "Building the project..."
mvn clean compile

echo ""
echo "Starting Filtered Trade Consumer for $SYMBOL..."
echo "==============================="
mvn exec:java -Dexec.mainClass="com.crypto.cli.FilteredTradeConsumer" -Dexec.args="$SYMBOL"