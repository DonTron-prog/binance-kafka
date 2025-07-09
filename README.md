# Binance Kafka Streaming Pipeline

A cryptocurrency trading data pipeline that streams live trades from Binance WebSocket API, publishes them to Kafka, and filters trades by symbol using Kafka Streams.

## Architecture

```
Binance WebSocket → Kafka (crypto-raw-trades) → Kafka Streams Filter → Kafka (filtered-trades-{symbol})
```

## Prerequisites

- Java 17+
- Docker and Docker Compose
- Maven 3.6+

## Quick Start

### 1. Start Kafka
```bash
docker compose up -d
```

### 2. Start the Binance Data Producer
This connects to Binance WebSocket API and streams live trades to the `crypto-raw-trades` topic:
```bash
mvn spring-boot:run
```

### 3. Verify Raw Trade Stream
In a new terminal, run the consumer to see live trades from Binance:
```bash
./run-consumer.sh
```

You should see live trade data from Binance streaming in your terminal.

### 4. Start the Filtering Application
Once you've verified the raw stream is working, start the filtering application to process trades by symbol:
```bash
# In a new terminal
java -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) com.crypto.streams.BinanceFilteringApp
```

### 5. Verify Filtered Streams
```bash
# in a new terminal
./run-filtered-conslumer.sh
```
The filtering application creates these output topics:
- `filtered-trades-btcusdt`: BTC/USDT trades only
- `filtered-trades-ethusdt`: ETH/USDT trades only  
- `filtered-trades-bnbusdt`: BNB/USDT trades only


## Key Components

### BinanceWebSocketClient.java
- Connects to Binance WebSocket API
- Streams live BTC/USDT, ETH/USDT, and BNB/USDT trades
- Publishes raw trades to `crypto-raw-trades` topic

### RawTradeConsumer.java  
- CLI tool to consume and display trades from `crypto-raw-trades` topic
- Useful for debugging and monitoring incoming data
- Run with: `./run-consumer.sh`

### BinanceFilteringApp.java
- Kafka Streams application that filters trades by symbol
- Reads from `crypto-raw-trades` topic
- Routes filtered trades to symbol-specific topics
- Uses JSON string matching to filter by symbol field

## Workflow

1. **Start the producer** (`mvn spring-boot:run`) to get live data from Binance
2. **Verify the raw stream** (`./run-consumer.sh`) to ensure trades are flowing
3. **Start the filtering app** to process trades by symbol
4. **Monitor filtered streams** by consuming from the filtered topics

## Stopping Applications

- Press `Ctrl+C` in each terminal to stop applications
- Stop Kafka: `docker compose down`

## Troubleshooting

- **No data streaming**: Make sure the producer is running first
- **Kafka connection errors**: Restart Kafka with `docker compose restart`
- **Port conflicts**: Kill processes using ports 8080/8081 with `lsof -i :8080` and `kill -9 <PID>`