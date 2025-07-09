#!/bin/bash

echo "This script will produce test messages to crypto-raw-trades topic"
echo "You can then see them filtered in the filtered-trades-btcusdt topic"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Create a sample trade message
while true; do
    TIMESTAMP=$(date +%s%3N)
    MESSAGE='{"e":"trade","E":'$TIMESTAMP',"s":"BTCUSDT","t":123456,"p":"43567.89","q":"0.0234","b":789012,"a":789013,"T":'$TIMESTAMP',"m":true}'
    
    echo "Sending: $MESSAGE"
    echo "$MESSAGE" | kafka-console-producer --broker-list localhost:9092 --topic crypto-raw-trades --property "parse.key=true" --property "key.separator=:" <<< "BTCUSDT:$MESSAGE"
    
    sleep 2
done