let stompClient = null;
let currentSubscription = null;
let tradeCount = 0;
const MAX_TRADES = 100; // Keep only the last 100 trades

function connect() {
    const socket = new SockJS('/ws');
    stompClient = Stomp.over(socket);
    
    stompClient.connect({}, function (frame) {
        console.log('Connected: ' + frame);
        updateConnectionStatus(true);
        
        // Subscribe to the default symbol
        const symbol = document.getElementById('symbol-select').value;
        subscribeToSymbol(symbol);
    }, function (error) {
        console.error('Connection error: ' + error);
        updateConnectionStatus(false);
        setTimeout(connect, 5000); // Retry connection after 5 seconds
    });
}

function updateConnectionStatus(connected) {
    const statusElement = document.getElementById('connection-status');
    if (connected) {
        statusElement.innerHTML = '<span class="badge bg-success">Connected</span>';
    } else {
        statusElement.innerHTML = '<span class="badge bg-danger">Disconnected</span>';
    }
}

function subscribeToSymbol(symbol) {
    // Unsubscribe from previous symbol if exists
    if (currentSubscription) {
        currentSubscription.unsubscribe();
    }
    
    // Reset trade count
    tradeCount = 0;
    updateTradeCount();
    
    // Clear the table
    const tableBody = document.getElementById('trades-table');
    tableBody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Waiting for trades...</td></tr>';
    
    // Subscribe to the new symbol
    const topic = `/topic/filtered-trades/${symbol}`;
    console.log(`Subscribing to ${topic}`);
    
    currentSubscription = stompClient.subscribe(topic, function (message) {
        const trade = JSON.parse(message.body);
        displayTrade(trade);
    });
}

function displayTrade(trade) {
    const tableBody = document.getElementById('trades-table');
    
    // Remove the "waiting" message if it exists
    if (tableBody.firstElementChild && tableBody.firstElementChild.textContent.includes('Waiting')) {
        tableBody.innerHTML = '';
    }
    
    // Create new row
    const row = document.createElement('tr');
    
    // Format time
    const time = moment(trade.tradeTime).format('HH:mm:ss.SSS');
    
    // Format price and quantity
    const price = parseFloat(trade.price).toFixed(2);
    const quantity = parseFloat(trade.quantity).toFixed(6);
    const volume = (parseFloat(trade.price) * parseFloat(trade.quantity)).toFixed(2);
    
    // Determine side (buy/sell)
    const side = trade.buyerMaker ? 'SELL' : 'BUY';
    const sideClass = trade.buyerMaker ? 'text-danger' : 'text-success';
    
    row.innerHTML = `
        <td>${time}</td>
        <td>$${price}</td>
        <td>${quantity}</td>
        <td>$${volume}</td>
        <td class="${sideClass} fw-bold">${side}</td>
    `;
    
    // Add animation class
    row.classList.add('table-row-new');
    
    // Insert at the top
    tableBody.insertBefore(row, tableBody.firstChild);
    
    // Remove animation class after animation completes
    setTimeout(() => row.classList.remove('table-row-new'), 1000);
    
    // Limit the number of rows
    while (tableBody.children.length > MAX_TRADES) {
        tableBody.removeChild(tableBody.lastChild);
    }
    
    // Update counters
    tradeCount++;
    updateTradeCount();
    updateLastUpdate();
}

function updateTradeCount() {
    document.getElementById('trade-count').textContent = tradeCount;
}

function updateLastUpdate() {
    document.getElementById('last-update').textContent = moment().format('HH:mm:ss');
}

// Symbol change handler
document.getElementById('symbol-select').addEventListener('change', function(e) {
    const newSymbol = e.target.value;
    console.log(`Switching to symbol: ${newSymbol}`);
    subscribeToSymbol(newSymbol);
});

// Initialize connection when page loads
document.addEventListener('DOMContentLoaded', function() {
    connect();
});