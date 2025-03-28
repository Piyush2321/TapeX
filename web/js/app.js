// DOM Elements
const refreshBtn = document.getElementById('refreshBtn');
const categoryFilter = document.getElementById('categoryFilter');
const transferTable = document.getElementById('transferTable');
const totalTransfers = document.getElementById('totalTransfers');
const totalSize = document.getElementById('totalSize');
const activeUsers = document.getElementById('activeUsers');

// State
let transfers = [];
let filteredTransfers = [];

// Utility Functions
function formatFileSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    if (bytes === 0) return '0 Byte';
    const i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
    return Math.round(bytes / Math.pow(1024, i), 2) + ' ' + sizes[i];
}

function formatTimestamp(timestamp) {
    return new Date(timestamp).toLocaleString();
}

// API Functions
async function fetchTransfers() {
    try {
        const response = await fetch('/api/transfers');
        if (!response.ok) throw new Error('Failed to fetch transfers');
        transfers = await response.json();
        updateUI();
    } catch (error) {
        console.error('Error fetching transfers:', error);
        alert('Failed to fetch transfers. Please try again.');
    }
}

// UI Update Functions
function updateStats() {
    const stats = filteredTransfers.reduce((acc, transfer) => {
        acc.totalTransfers++;
        acc.totalSize += transfer.Filesize_in_bytes;
        acc.users.add(transfer.username);
        return acc;
    }, { totalTransfers: 0, totalSize: 0, users: new Set() });

    totalTransfers.textContent = stats.totalTransfers;
    totalSize.textContent = formatFileSize(stats.totalSize);
    activeUsers.textContent = stats.users.size;
}

function updateTable() {
    const tbody = transferTable.querySelector('tbody');
    tbody.innerHTML = '';

    filteredTransfers.forEach(transfer => {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${formatTimestamp(transfer.timestamp)}</td>
            <td>${transfer.username}</td>
            <td>${transfer.Filename}</td>
            <td>${formatFileSize(transfer.Filesize_in_bytes)}</td>
            <td>${transfer.file_destination}</td>
            <td>${transfer.File_in_or_out}</td>
        `;
        tbody.appendChild(row);
    });
}

function filterTransfers() {
    const category = categoryFilter.value;
    filteredTransfers = category === 'all'
        ? transfers
        : transfers.filter(transfer => transfer.file_destination === category);
}

function updateUI() {
    filterTransfers();
    updateStats();
    updateTable();
}

// Event Listeners
refreshBtn.addEventListener('click', fetchTransfers);
categoryFilter.addEventListener('change', updateUI);

// Initial Load
fetchTransfers(); 