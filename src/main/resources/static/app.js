// WebSocket connection
let stompClient = null;
let currentSymbol = 'BTCUSDT';
let subscriptions = {};

// Chart configurations
let priceChart = null;
let volumeChart = null;
const WINDOW_MINUTES = 10;
const maxDataPoints = WINDOW_MINUTES * 60; // 10 minutes * 60 seconds = 600 points max

// Data management
let priceDataBuffer = [];
let volumeDataBuffer = { buy: [], sell: [] };
let aggregatedVolumeBuffer = [];
let lastUpdateTime = 0;

// Remove data older than 10 minutes
function cleanOldData() {
    const tenMinutesAgo = new Date(Date.now() - WINDOW_MINUTES * 60 * 1000);
    
    // Clean price data
    priceDataBuffer = priceDataBuffer.filter(point => point.x > tenMinutesAgo);
    
    // Clean volume data  
    volumeDataBuffer.buy = volumeDataBuffer.buy.filter(point => point.x > tenMinutesAgo);
    volumeDataBuffer.sell = volumeDataBuffer.sell.filter(point => point.x > tenMinutesAgo);
    aggregatedVolumeBuffer = aggregatedVolumeBuffer.filter(point => point.x > tenMinutesAgo);
}

// Aggregate volume data into 30-second buckets for better visualization
function aggregateVolumeData() {
    const bucketSize = 30000; // 30 seconds in milliseconds
    const buckets = new Map();
    
    // Aggregate buy volume
    volumeDataBuffer.buy.forEach(point => {
        const bucketTime = Math.floor(point.x.getTime() / bucketSize) * bucketSize;
        const key = `${bucketTime}_buy`;
        if (!buckets.has(key)) {
            buckets.set(key, { x: new Date(bucketTime), y: 0, type: 'buy' });
        }
        buckets.get(key).y += point.y;
    });
    
    // Aggregate sell volume
    volumeDataBuffer.sell.forEach(point => {
        const bucketTime = Math.floor(point.x.getTime() / bucketSize) * bucketSize;
        const key = `${bucketTime}_sell`;
        if (!buckets.has(key)) {
            buckets.set(key, { x: new Date(bucketTime), y: 0, type: 'sell' });
        }
        buckets.get(key).y += point.y;
    });
    
    // Convert to separate arrays for Chart.js
    const buyData = Array.from(buckets.values())
        .filter(bucket => bucket.type === 'buy')
        .sort((a, b) => a.x - b.x);
    
    const sellData = Array.from(buckets.values())
        .filter(bucket => bucket.type === 'sell')
        .sort((a, b) => a.x - b.x);
    
    return { buyData, sellData };
}

// Update charts with buffered data
function updateChartsFromBuffer(forceUpdate = false) {
    // Throttle updates to prevent excessive redraws (but allow forced updates)
    const now = Date.now();
    if (!forceUpdate && now - lastUpdateTime < 500) return; // Update at most twice per second
    lastUpdateTime = now;
    
    if (priceChart && priceChart.data.datasets[0]) {
        // Sort data by timestamp to ensure proper display
        const sortedPriceData = [...priceDataBuffer].sort((a, b) => a.x - b.x);
        priceChart.data.datasets[0].data = sortedPriceData;
        
        // Update chart options to reflect current time window
        const tenMinutesAgo = new Date(Date.now() - WINDOW_MINUTES * 60 * 1000);
        priceChart.options.scales.x.min = tenMinutesAgo;
        priceChart.options.scales.x.max = new Date();
        
        priceChart.update('none');
        console.log('📊 Price chart updated with', sortedPriceData.length, 'points');
    }
    
    if (volumeChart) {
        const { buyData, sellData } = aggregateVolumeData();
        if (volumeChart.data.datasets[0]) volumeChart.data.datasets[0].data = buyData;
        if (volumeChart.data.datasets[1]) volumeChart.data.datasets[1].data = sellData;
        
        // Update chart options to reflect current time window
        const tenMinutesAgo = new Date(Date.now() - WINDOW_MINUTES * 60 * 1000);
        volumeChart.options.scales.x.min = tenMinutesAgo;
        volumeChart.options.scales.x.max = new Date();
        
        volumeChart.update('none');
        console.log('📊 Volume chart updated - Buy buckets:', buyData.length, 'Sell buckets:', sellData.length);
    }
}

// Initialize charts
function initCharts() {
    // Price chart
    const priceCtx = document.getElementById('price-chart').getContext('2d');
    priceChart = new Chart(priceCtx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Price',
                data: [],
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.1)',
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                fill: false,
                tension: 0.1,
                showLine: true,
                spanGaps: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            aspectRatio: 2,
            animation: {
                duration: 0
            },
            interaction: {
                intersect: false,
                mode: 'index'
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute',
                        displayFormats: {
                            minute: 'HH:mm'
                        },
                        tooltipFormat: 'HH:mm:ss'
                    },
                    ticks: {
                        source: 'auto',
                        autoSkip: true,
                        maxTicksLimit: 10
                    }
                },
                y: {
                    position: 'right',
                    beginAtZero: false
                }
            },
            plugins: {
                legend: {
                    display: false
                }
            },
            elements: {
                point: {
                    radius: 2,
                    hoverRadius: 4
                }
            }
        }
    });

    // Volume chart
    const volumeCtx = document.getElementById('volume-chart').getContext('2d');
    volumeChart = new Chart(volumeCtx, {
        type: 'bar',
        data: {
            datasets: [{
                label: 'Buy Volume',
                data: [],
                backgroundColor: 'rgba(75, 192, 192, 0.8)',
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1,
                stack: 'volume'
            }, {
                label: 'Sell Volume',
                data: [],
                backgroundColor: 'rgba(255, 99, 132, 0.8)',
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1,
                stack: 'volume'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            aspectRatio: 2,
            animation: {
                duration: 0
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'minute',
                        displayFormats: {
                            minute: 'HH:mm'
                        }
                    },
                    min: function() {
                        return new Date(Date.now() - WINDOW_MINUTES * 60 * 1000);
                    },
                    max: function() {
                        return new Date();
                    },
                    stacked: true
                },
                y: {
                    stacked: true,
                    beginAtZero: true
                }
            }
        }
    });
}

// Connect to WebSocket
function connect() {
    console.log('Attempting to connect to WebSocket...');
    updateConnectionStatus(false);
    
    // Close existing connection if any
    if (stompClient && stompClient.connected) {
        stompClient.disconnect();
    }
    
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    
    // Disable debug logging for cleaner console
    stompClient.debug = null;
    
    stompClient.connect({}, function(frame) {
        console.log('Connected to WebSocket: ' + frame);
        updateConnectionStatus(true);
        subscribeToSymbol(currentSymbol);
        
        // Start connection health checks
        startHealthCheck();
    }, function(error) {
        console.error('WebSocket connection error: ', error);
        updateConnectionStatus(false);
        
        // Exponential backoff for reconnection
        setTimeout(connect, Math.min(30000, 5000 * Math.pow(2, getReconnectAttempts())));
        incrementReconnectAttempts();
    });
}

let reconnectAttempts = 0;
let healthCheckInterval = null;

function getReconnectAttempts() {
    return Math.min(reconnectAttempts, 5); // Cap at 5 for max 5000 * 2^5 = 160 seconds
}

function incrementReconnectAttempts() {
    reconnectAttempts++;
}

function resetReconnectAttempts() {
    reconnectAttempts = 0;
}

function startHealthCheck() {
    resetReconnectAttempts();
    
    if (healthCheckInterval) {
        clearInterval(healthCheckInterval);
    }
    
    healthCheckInterval = setInterval(function() {
        fetch('/api/status')
            .then(response => response.json())
            .then(data => {
                if (data.connected && data.healthy) {
                    updateConnectionStatus(true);
                } else {
                    updateConnectionStatus(false);
                    console.log('Backend connection unhealthy, status:', data);
                }
            })
            .catch(error => {
                console.error('Health check failed:', error);
                updateConnectionStatus(false);
            });
    }, 10000); // Check every 10 seconds
}

// Subscribe to symbol data
function subscribeToSymbol(symbol) {
    // Unsubscribe from previous subscriptions
    Object.values(subscriptions).forEach(sub => sub.unsubscribe());
    subscriptions = {};

    // Subscribe to price updates
    subscriptions.prices = stompClient.subscribe(`/topic/prices/${symbol}`, function(message) {
        const price = JSON.parse(message.body);
        updatePriceData(price);
    });

    // Subscribe to trend updates
    subscriptions.trends = stompClient.subscribe(`/topic/trends/${symbol}`, function(message) {
        const trend = JSON.parse(message.body);
        updateTrendData(trend);
    });

    // Subscribe to volume analytics
    subscriptions.volume = stompClient.subscribe(`/topic/volume/${symbol}`, function(message) {
        const volume = JSON.parse(message.body);
        updateVolumeData(volume);
    });

    // Subscribe to alerts
    subscriptions.alerts = stompClient.subscribe(`/topic/alerts/${symbol}`, function(message) {
        const alert = JSON.parse(message.body);
        addAlert(alert);
    });
    
    // Temporary: Subscribe to raw trades for testing
    subscriptions.rawTrades = stompClient.subscribe(`/topic/trades/${symbol}`, function(message) {
        const trade = JSON.parse(message.body);
        console.log('🔥 Received raw trade:', trade);
        
        // Update price display with raw trade data
        document.getElementById('current-price').textContent = '$' + parseFloat(trade.price).toFixed(2);
        document.getElementById('volume').textContent = formatNumber(trade.volume || trade.quantity);
        
        // Add to price buffer and update chart (with validation)
        const now = new Date();
        const priceValue = parseFloat(trade.price);
        
        // Only add valid price data
        if (!isNaN(priceValue) && priceValue > 0) {
            const pricePoint = {
                x: now,
                y: priceValue
            };
            
            priceDataBuffer.push(pricePoint);
            
            // Limit buffer size to prevent memory issues
            if (priceDataBuffer.length > maxDataPoints) {
                priceDataBuffer = priceDataBuffer.slice(-maxDataPoints);
            }
        } else {
            console.warn('⚠️ Invalid price data received:', trade.price);
        }
        
        // Create volume data from individual trades (with validation)
        const quantity = parseFloat(trade.quantity || 0);
        const price = parseFloat(trade.price || 0);
        const volumeValue = quantity * price; // Calculate volume in USD
        const isBuy = !trade.isBuyerMaker; // If buyer is NOT maker, it's a buy order
        
        // Only add valid volume data
        if (!isNaN(volumeValue) && volumeValue > 0 && !isNaN(quantity) && quantity > 0) {
            const volumePoint = {
                x: now,
                y: volumeValue
            };
            
            if (isBuy) {
                volumeDataBuffer.buy.push(volumePoint);
                console.log('📈 Buy volume:', volumeValue.toFixed(2), 'USD');
            } else {
                volumeDataBuffer.sell.push(volumePoint);
                console.log('📉 Sell volume:', volumeValue.toFixed(2), 'USD');
            }
        } else {
            console.warn('⚠️ Invalid volume data received - quantity:', trade.quantity, 'price:', trade.price);
        }
        
        // Clean old data and update chart immediately for price updates
        cleanOldData();
        updateChartsFromBuffer(true); // Force update for price data
        
        console.log('💰 Price updated:', trade.symbol, '@', parseFloat(trade.price), 'Buffer sizes - Price:', priceDataBuffer.length, 'Buy:', volumeDataBuffer.buy.length, 'Sell:', volumeDataBuffer.sell.length);
    });
}

// Update price data and chart
function updatePriceData(price) {
    // Update current price display
    const priceElement = document.getElementById('current-price');
    priceElement.textContent = `$${parseFloat(price.close).toFixed(2)}`;
    
    // Update price change
    const changeElement = document.getElementById('price-change');
    const changePercent = parseFloat(price.priceChangePercent);
    changeElement.textContent = `${changePercent.toFixed(2)}%`;
    changeElement.className = changePercent >= 0 ? 'stat-value price-up' : 'stat-value price-down';
    
    // Update volume
    document.getElementById('volume').textContent = `$${formatNumber(price.volume)}`;
    
    // Add aggregated price data to chart (with validation)
    const now = new Date();
    const priceValue = parseFloat(price.close);
    
    // Only add valid price data
    if (!isNaN(priceValue) && priceValue > 0) {
        const pricePoint = {
            x: now,
            y: priceValue
        };
        
        priceDataBuffer.push(pricePoint);
        
        // Limit buffer size
        if (priceDataBuffer.length > maxDataPoints) {
            priceDataBuffer = priceDataBuffer.slice(-maxDataPoints);
        }
        
        // Update charts
        cleanOldData();
        updateChartsFromBuffer(true);
        
        console.log('📊 Aggregated price added to chart:', price.symbol, '@', priceValue);
    } else {
        console.warn('⚠️ Invalid aggregated price data received:', price.close);
    }
}

// Update trend data
function updateTrendData(trend) {
    // Update RSI
    const rsiElement = document.getElementById('rsi');
    if (trend.rsi) {
        const rsi = parseFloat(trend.rsi);
        rsiElement.textContent = rsi.toFixed(2);
        if (rsi > 70) {
            rsiElement.className = 'stat-value price-down';
        } else if (rsi < 30) {
            rsiElement.className = 'stat-value price-up';
        } else {
            rsiElement.className = 'stat-value';
        }
    }
    
    // Update trend direction
    const trendElement = document.getElementById('trend');
    if (trend.trendDirection) {
        trendElement.textContent = `${trend.trendDirection} (${trend.trendStrength})`;
        trendElement.className = `stat-value trend-${trend.trendDirection.toLowerCase()}`;
    }
}

// Update volume data
function updateVolumeData(volume) {
    const now = new Date();
    
    // Validate and add to volume buffer
    const buyVolume = parseFloat(volume.buyVolume || 0);
    const sellVolume = parseFloat(volume.sellVolume || 0);
    
    if (!isNaN(buyVolume) && buyVolume >= 0) {
        volumeDataBuffer.buy.push({
            x: now,
            y: buyVolume
        });
    }
    
    if (!isNaN(sellVolume) && sellVolume >= 0) {
        volumeDataBuffer.sell.push({
            x: now,
            y: sellVolume
        });
    }
    
    // Clean old data and update chart
    cleanOldData();
    updateChartsFromBuffer();
    
    console.log('📊 Volume updated:', volume.symbol, 'Buy:', buyVolume, 'Sell:', sellVolume, 'Buffer sizes:', volumeDataBuffer.buy.length, volumeDataBuffer.sell.length);
}

// Add alert to the alerts container
function addAlert(alert) {
    const container = document.getElementById('alerts-container');
    const alertDiv = document.createElement('div');
    
    const severityClass = `alert-${alert.severity.toLowerCase()}`;
    alertDiv.className = `alert-item ${severityClass}`;
    
    const time = new Date(alert.timestamp).toLocaleTimeString();
    alertDiv.innerHTML = `
        <strong>${time}</strong> - ${alert.alertType}<br>
        ${alert.message}
    `;
    
    container.insertBefore(alertDiv, container.firstChild);
    
    // Keep only last 20 alerts
    while (container.children.length > 20) {
        container.removeChild(container.lastChild);
    }
}

// Update connection status
function updateConnectionStatus(connected) {
    const statusElement = document.getElementById('connection-status').querySelector('.badge');
    if (connected) {
        statusElement.textContent = 'Connected';
        statusElement.className = 'badge bg-success';
    } else {
        statusElement.textContent = 'Disconnected';
        statusElement.className = 'badge bg-danger';
        
        // Try to reconnect if we're disconnected
        if (stompClient && !stompClient.connected) {
            console.log('Detected disconnection, attempting to reconnect...');
            setTimeout(connect, 2000);
        }
    }
}

// Format large numbers
function formatNumber(num) {
    const n = parseFloat(num);
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
    return n.toFixed(2);
}

// Clear all chart data
function clearCharts() {
    // Clear data buffers
    priceDataBuffer = [];
    volumeDataBuffer = { buy: [], sell: [] };
    aggregatedVolumeBuffer = [];
    
    // Clear chart displays
    if (priceChart && priceChart.data.datasets[0]) {
        priceChart.data.datasets[0].data = [];
        priceChart.update('none');
    }
    if (volumeChart) {
        if (volumeChart.data.datasets[0]) volumeChart.data.datasets[0].data = [];
        if (volumeChart.data.datasets[1]) volumeChart.data.datasets[1].data = [];
        volumeChart.update('none');
    }
}

// Handle symbol selection change
document.getElementById('symbol-select').addEventListener('change', function(e) {
    currentSymbol = e.target.value;
    
    // Clear charts completely
    clearCharts();
    
    // Clear stats
    document.getElementById('current-price').textContent = '-';
    document.getElementById('price-change').textContent = '-';
    document.getElementById('volume').textContent = '-';
    document.getElementById('rsi').textContent = '-';
    document.getElementById('trend').textContent = '-';
    
    // Clear alerts
    document.getElementById('alerts-container').innerHTML = '';
    
    // Subscribe to new symbol
    if (stompClient && stompClient.connected) {
        subscribeToSymbol(currentSymbol);
    }
});

// Generate test data for charts when no real data is available
function generateTestData() {
    const now = new Date();
    
    // Generate realistic price data based on current symbol
    let basePrice = 45000; // Default BTC price
    if (currentSymbol === 'ETHUSDT') basePrice = 2500;
    if (currentSymbol === 'BNBUSDT') basePrice = 400;
    
    // Generate price with small realistic variations
    const priceVariation = basePrice * 0.001; // 0.1% variation
    const price = basePrice + (Math.random() - 0.5) * priceVariation;
    
    const pricePoint = {
        x: now,
        y: Math.round(price * 100) / 100 // Round to 2 decimal places
    };
    priceDataBuffer.push(pricePoint);
    
    // Generate realistic volume data
    const buyVolume = Math.random() * 50 + 10; // 10-60 volume
    const sellVolume = Math.random() * 50 + 10; // 10-60 volume
    
    volumeDataBuffer.buy.push({
        x: now,
        y: Math.round(buyVolume * 100) / 100
    });
    
    volumeDataBuffer.sell.push({
        x: now,
        y: Math.round(sellVolume * 100) / 100
    });
    
    // Update UI
    document.getElementById('current-price').textContent = '$' + pricePoint.y.toFixed(2);
    document.getElementById('volume').textContent = formatNumber(buyVolume + sellVolume);
    
    console.log('📊 Generated test data - Price:', pricePoint.y, 'Buy Vol:', buyVolume.toFixed(2), 'Sell Vol:', sellVolume.toFixed(2));
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initCharts();
    connect();
    
    // Generate test data every 2 seconds if no real data is coming in
    let testDataInterval = setInterval(() => {
        if (priceDataBuffer.length < 3) { // Only generate if we have minimal real data
            generateTestData();
            cleanOldData();
            updateChartsFromBuffer(true);
            console.log('🧪 Test data generated - buffer now has', priceDataBuffer.length, 'price points');
        }
    }, 2000);
    
    // Update charts every 2 seconds to maintain 10-minute sliding window
    setInterval(() => {
        cleanOldData();
        updateChartsFromBuffer(true); // Force update every 2 seconds
        console.log('🔄 Charts refreshed - Price points:', priceDataBuffer.length, 'Volume points:', volumeDataBuffer.buy.length);
        
        // Debug: Show sample data points
        if (priceDataBuffer.length > 0) {
            console.log('💰 Latest price point:', priceDataBuffer[priceDataBuffer.length - 1]);
        }
        if (volumeDataBuffer.buy.length > 0) {
            console.log('📈 Latest buy volume:', volumeDataBuffer.buy[volumeDataBuffer.buy.length - 1]);
        }
        if (volumeDataBuffer.sell.length > 0) {
            console.log('📉 Latest sell volume:', volumeDataBuffer.sell[volumeDataBuffer.sell.length - 1]);
        }
        
        // Stop test data generation if we have enough real data
        if (priceDataBuffer.length > 20 && testDataInterval) {
            clearInterval(testDataInterval);
            testDataInterval = null;
            console.log('🎯 Real data detected, stopping test data generation');
        }
    }, 2000);
});