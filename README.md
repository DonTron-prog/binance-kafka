# Binance Kafka Streams - Real-Time Cryptocurrency Analytics

A real-time cryptocurrency data processing pipeline that streams live price data from Binance exchange through Apache Kafka and processes it using Kafka Streams for analytics and trend detection.

## Architecture Overview

```
Binance WebSocket API → Kafka Producer → Kafka Topics → Kafka Streams Processing → WebSocket Server → Real-time Dashboard
```

## Features

- **Real-time Data Ingestion**: Connects to Binance WebSocket API for live cryptocurrency data
- **Stream Processing**: Uses Kafka Streams for real-time analytics including:
  - Price aggregations (1min, 5min, 15min windows)
  - Volume analysis and buy/sell pressure
  - Trend detection with technical indicators (SMA, EMA, RSI)
  - Anomaly detection and alerts
- **Real-time Visualization**: Web dashboard with live charts and alerts
- **Scalable Architecture**: Built on Apache Kafka for high throughput and fault tolerance

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
   java -jar target/binance-kafka-streams-1.0.0.jar --server.port=8081
   ```

4. **Access the Dashboard**
   - Open http://localhost:8080 in your browser
   - The dashboard will automatically connect to the WebSocket server

## Configuration

Key configuration options in `application.yml`:

- **Binance WebSocket**: Configure symbols and streams to subscribe to
- **Kafka Topics**: Define topic names for different data streams
- **Stream Processing**: Adjust window sizes and processing parameters

## Project Structure

```
src/main/java/com/crypto/
├── model/              # Domain models (Trade, AggregatedPrice, etc.)
├── binance/           # Binance WebSocket client
├── streams/           # Kafka Streams processing applications
├── websocket/         # WebSocket server for real-time data delivery
├── controller/        # REST endpoints
└── config/           # Configuration classes
```

## Kafka Topics

- `crypto-raw-trades`: Raw trade data from Binance
- `crypto-aggregated-prices`: OHLCV data for different time windows
- `crypto-volume-analytics`: Volume analysis and buy/sell pressure
- `crypto-price-trends`: Technical indicators and trend analysis
- `crypto-alerts`: Real-time alerts for significant events

## Technical Indicators

The system calculates various technical indicators:
- Simple Moving Averages (SMA): 5, 15, 30 periods
- Exponential Moving Averages (EMA): 5, 15 periods
- Relative Strength Index (RSI)
- Volume-Weighted Average Price (VWAP)
- Momentum indicators

## Alerts

The system generates alerts for:
- Price spikes (>2% change in 1 minute)
- Golden/Death cross patterns
- Overbought/Oversold conditions (RSI)
- Unusual volume activity
- Heavy buying/selling pressure

## Monitoring

- Health endpoint: http://localhost:8080/health
- Metrics endpoint: http://localhost:8080/actuator/metrics
- Prometheus metrics: http://localhost:8080/actuator/prometheus
- Kafka UI: http://localhost:8090 (when using docker-compose)

## Development

To add new cryptocurrencies, update the `binance.websocket.symbols` configuration in `application.yml`.

To add new stream processing logic, create a new class in the `streams` package and annotate methods with `@Autowired` to build the pipeline.

## Troubleshooting

- **Connection Issues**: Check if Kafka is running and accessible
- **No Data**: Verify Binance WebSocket URL and network connectivity
- **Processing Delays**: Monitor Kafka consumer lag through Kafka UI

## License

This project is for educational purposes. Please comply with Binance API terms of service when using in production.
