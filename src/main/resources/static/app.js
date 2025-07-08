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
        console.log('Received trade:', trade);
        displayTrade(trade);
    });
}

function displayTrade(trade) {
    const tableBody = document.getElementById('trades-table');
    
    // Remove the "waiting" message if it exists
    if (tableBody.firstElementChild && tableBody.firstElementChild.textContent.includes('Waiting')) {
        tableBody.innerHTML = '';
    }
    
    // Debug: log the trade object structure
    console.log('Trade object keys:', Object.keys(trade));
    console.log('Full trade object:', JSON.stringify(trade, null, 2));
    
    // Create new row
    const row = document.createElement('tr');
    
    // Check all possible field names for price
    const priceValue = trade.price || trade.p || trade.Price || 0;
    const quantityValue = trade.quantity || trade.q || trade.Quantity || 0;
    const tradeTimeValue = trade.tradeTime || trade.T || trade.TradeTime || Date.now();
    const buyerMakerValue = trade.buyerMaker !== undefined ? trade.buyerMaker : 
                           (trade.isBuyerMaker !== undefined ? trade.isBuyerMaker : 
                           (trade.m !== undefined ? trade.m : false));
    
    console.log('Extracted values:', {
        price: priceValue,
        quantity: quantityValue,
        tradeTime: tradeTimeValue,
        buyerMaker: buyerMakerValue
    });
    
    // Format time
    const time = moment(tradeTimeValue).format('HH:mm:ss.SSS');
    
    // Format price and quantity - handle both string and number formats
    const price = priceValue ? parseFloat(priceValue).toFixed(2) : '0.00';
    const quantity = quantityValue ? parseFloat(quantityValue).toFixed(6) : '0.000000';
    const volume = (priceValue && quantityValue) ? 
        (parseFloat(priceValue) * parseFloat(quantityValue)).toFixed(2) : '0.00';
    
    // Determine side (buy/sell)
    const side = buyerMakerValue ? 'SELL' : 'BUY';
    const sideClass = buyerMakerValue ? 'text-danger' : 'text-success';
    
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