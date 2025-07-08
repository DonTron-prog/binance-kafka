# Kafka Streams Symbol Filter Demo

A simplified demonstration of Kafka Streams filtering capabilities that streams live cryptocurrency data from Binance and filters trades by symbol.

## Overview

This demo application showcases:
- **Kafka Streams Filtering**: Simple symbol-based filtering of trade data
- **Real-time Processing**: Live cryptocurrency trades from Binance WebSocket API
- **WebSocket Delivery**: Filtered trades delivered to web dashboard via WebSocket

## Architecture

```
Binance WebSocket → Kafka (raw-trades) → Symbol Filter Stream → Kafka (filtered-trades-{symbol}) → WebSocket → Dashboard
```

## Features

- Connects to Binance WebSocket API for BTC/USDT, ETH/USDT, and BNB/USDT
- Filters trades by symbol using Kafka Streams
- Displays real-time filtered trade feed in web interface
- Shows trade time, price, quantity, volume, and buy/sell side

## Prerequisites

- Java 17+
- Docker and Docker Compose
- Maven 3.6+

## Quick Start

1. **Start Kafka**
   ```bash
   docker compose up -d
   ```

2. **Build the Application**
   ```bash
   mvn clean package
   ```

3. **Run the Application**
   ```bash
   java -jar target/binance-kafka-streams-1.0.0.jar
   ```

4. **Access the Dashboard**
   - Open http://localhost:8081 in your browser
   - Select a symbol from the dropdown to see filtered trades

## Kafka Topics

- `crypto-raw-trades`: All incoming trades from Binance
- `crypto-filtered-trades-btcusdt`: Filtered BTC/USDT trades only
- `crypto-filtered-trades-ethusdt`: Filtered ETH/USDT trades only  
- `crypto-filtered-trades-bnbusdt`: Filtered BNB/USDT trades only

## Project Structure

```
src/main/java/com/crypto/
├── streams/
│   └── SymbolFilterStream.java    # Kafka Streams filter implementation
├── websocket/
│   └── MarketDataPublisher.java   # Publishes filtered trades to WebSocket
└── model/
    └── Trade.java                 # Trade data model
```

## How the Filter Works

The `SymbolFilterStream` class:
1. Reads from the `crypto-raw-trades` topic
2. Filters trades based on the symbol field
3. Writes filtered trades to symbol-specific topics

```java
tradesStream
    .filter((key, trade) -> "BTCUSDT".equals(trade.getSymbol()))
    .to(filteredTradesBtcTopic);
```

## Dashboard

The web dashboard provides:
- Symbol selector (BTC/USDT, ETH/USDT, BNB/USDT)
- Real-time filtered trade feed
- Trade count and last update time
- Color-coded buy (green) and sell (red) indicators

## Development

To modify the filtering logic, edit `SymbolFilterStream.java`. The filter predicates can be customized to filter by price, volume, or any other trade attribute.

## License

This project is for educational purposes.