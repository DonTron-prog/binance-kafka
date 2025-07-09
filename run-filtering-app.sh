#!/bin/bash

# Script to run the Binance Filtering Kafka Streams Application

echo "Building the project..."
mvn clean compile

echo ""
echo "Starting Binance Filtering Kafka Streams Application..."
echo "=================================================="
echo "This will filter trades from 'crypto-raw-trades' topic"
echo "and route them to:"
echo "  - filtered-trades-btcusdt"
echo "  - filtered-trades-ethusdt"
echo "  - filtered-trades-bnbusdt"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Run the filtering app
mvn exec:java -Dexec.mainClass="com.crypto.streams.BinanceFilteringApp"