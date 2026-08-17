// --- WebSocket and Dashboard Logic ---

// DOM Elements
const wsStatus = document.getElementById('ws-status');
const terminal = document.getElementById('log-terminal');
const logCounter = document.getElementById('log-counter');
const clearTerminalBtn = document.getElementById('btn-clear-terminal');

const throughputValue = document.querySelector('#metric-throughput .metric-value');
const successValue = document.querySelector('#metric-success .metric-value');
const latencyValue = document.querySelector('#metric-latency .metric-value');
const dlqValue = document.querySelector('#metric-dlq .metric-value');

// Simulator Buttons
const simHealthyBtn = document.getElementById('btn-sim-healthy');
const simErrorBtn = document.getElementById('btn-sim-error');
const simCorruptBtn = document.getElementById('btn-sim-corrupt');

// State Variables
let socket = null;
let eventCount = 0;
let dlqCount = 0;
let healthyCount = 0;
let rollingEvents = 0;
let lastCountReset = Date.now();
let recentResponseTimes = [];
let activeParticles = 0;

// Color constants
const COLORS = {
    info: 'var(--color-green)',
    warn: 'var(--color-orange)',
    error: 'var(--color-red)',
    system: 'var(--color-cyan)',
    raw: 'var(--color-purple)'
};

// WebSocket Management
function connect() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    
    appendSystemLog(`Attempting connection to WebSocket at ${wsUrl}...`);
    socket = new WebSocket(wsUrl);

    socket.onopen = () => {
        wsStatus.className = 'status-badge connected';
        wsStatus.querySelector('.status-text').textContent = 'Connected';
        appendSystemLog('WebSocket connection established. Live log stream active!');
    };

    socket.onclose = () => {
        wsStatus.className = 'status-badge disconnected';
        wsStatus.querySelector('.status-text').textContent = 'Disconnected';
        appendSystemLog('WebSocket connection lost. Reconnecting in 3s...');
        setTimeout(connect, 3000);
    };

    socket.onerror = (error) => {
        console.error('WebSocket Error: ', error);
    };

    socket.onmessage = (messageEvent) => {
        try {
            const log = JSON.parse(messageEvent.data);
            handleIncomingLog(log);
        } catch (e) {
            console.error('Failed to parse websocket frame: ', e);
        }
    };
}

// Ingestion Handler
function handleIncomingLog(log) {
    eventCount++;
    rollingEvents++;
    logCounter.textContent = `${eventCount} events`;
    
    // Determine level and check if it's a schema issue / DLQ log
    let level = 'INFO';
    let isError = false;
    let isDlq = false;
    
    if (log.error_reason || log.error_stage) {
        isDlq = true;
        dlqCount++;
        dlqValue.textContent = dlqCount;
    } else {
        level = (log.level || log.log_level || 'INFO').toUpperCase();
        isError = log.is_error || level === 'ERROR' || level === 'FATAL' || (log.status_code && log.status_code >= 500);
        if (!isError) {
            healthyCount++;
        }
    }
    
    // Track latency
    if (log.response_time_ms) {
        recentResponseTimes.push(log.response_time_ms);
        if (recentResponseTimes.length > 50) recentResponseTimes.shift();
        const avgLatency = Math.round(recentResponseTimes.reduce((a, b) => a + b, 0) / recentResponseTimes.length);
        latencyValue.innerHTML = `${avgLatency} <span class="unit">ms</span>`;
    }

    // Format & Append log text
    appendLogLine(log, level, isError, isDlq);
    
    // Trigger SVG particle animation
    animatePipelineFlow(log, isError, isDlq);
}

// Log Terminal Rendering
function appendLogLine(log, level, isError, isDlq) {
    const timeStr = new Date().toTimeString().split(' ')[0];
    const line = document.createElement('div');
    
    if (isDlq) {
        line.className = 'log-line error-log';
        line.innerHTML = `<span class="time">${timeStr}</span> <span class="tag">[DLQ]</span> <span class="msg">REJECTED [Stage: ${log.error_stage}]: ${log.error_reason} | Event ID: ${log.event_id || 'none'}</span>`;
    } else {
        let classType = 'info-log';
        if (level === 'WARN') classType = 'warn-log';
        if (isError) classType = 'error-log';
        
        const service = log.service || 'unknown-service';
        const msg = log.message || log.raw_message || JSON.stringify(log);
        const status = log.status_code ? ` (Status: ${log.status_code})` : '';
        const latency = log.response_time_ms ? ` in ${log.response_time_ms}ms` : '';
        
        line.className = `log-line ${classType}`;
        line.innerHTML = `<span class="time">${timeStr}</span> <span class="tag">[${level}]</span> <span class="msg"><strong>${service}</strong>: ${msg}${status}${latency}</span>`;
    }
    
    terminal.appendChild(line);
    while (terminal.childElementCount > 50) {
        terminal.removeChild(terminal.firstChild);
    }
    // Keep container scrolled to bottom
    terminal.scrollTop = terminal.scrollHeight;
}

function appendSystemLog(msg) {
    const timeStr = new Date().toTimeString().split(' ')[0];
    const line = document.createElement('div');
    line.className = 'log-line system-msg';
    line.innerHTML = `<span class="time">${timeStr}</span> <span class="tag">[SYS]</span> <span class="msg">${msg}</span>`;
    terminal.appendChild(line);
    while (terminal.childElementCount > 50) {
        terminal.removeChild(terminal.firstChild);
    }
    terminal.scrollTop = terminal.scrollHeight;
}

// Pre-calculated Path Cache to avoid calling getPointAtLength on every frame
const PATH_CACHE = {};

function precalculatePaths() {
    const paths = [
        'path-source-gateway',
        'path-gateway-raw',
        'path-raw-processor',
        'path-processor-parsed',
        'path-processor-dlq',
        'path-parsed-clickhouse',
        'path-dlq-clickhouse'
    ];
    paths.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            const len = el.getTotalLength();
            const samples = [];
            const numSamples = 100;
            for (let i = 0; i <= numSamples; i++) {
                const pt = el.getPointAtLength((i / numSamples) * len);
                samples.push({ x: pt.x, y: pt.y });
            }
            PATH_CACHE[id] = samples;
        }
    });
}

// Particle flow engine along SVG Paths
async function animatePipelineFlow(log, isError, isDlq) {
    if (activeParticles >= 8) {
        // Skip rendering particle if too many are already active to prevent lag
        return;
    }
    activeParticles++;
    const particlesGroup = document.getElementById('particles-group');
    
    // Node pulse helper
    const pulseNode = (groupIndex, colorClass) => {
        const groups = document.querySelectorAll('.node-group');
        if (groups[groupIndex]) {
            groups[groupIndex].classList.add(`pulse-${colorClass}`);
            setTimeout(() => groups[groupIndex].classList.remove(`pulse-${colorClass}`), 1000);
        }
    };

    // Determine paths based on log status
    const segments = [
        'path-source-gateway',
        'path-gateway-raw',
        'path-raw-processor'
    ];
    
    if (isDlq) {
        segments.push('path-processor-dlq');
        segments.push('path-dlq-clickhouse');
    } else {
        segments.push('path-processor-parsed');
        segments.push('path-parsed-clickhouse');
    }
    
    // Choose particle theme
    let theme = 'info';
    if (isError) theme = 'error';
    if (isDlq) theme = 'error';

    // Animate a single particle sequentially along the segment paths
    let currentSegment = 0;

    function runNextSegment() {
        if (currentSegment >= segments.length) return;
        
        const pathId = segments[currentSegment];
        const pathEl = document.getElementById(pathId);
        if (!pathEl) return;
        
        // Create circle
        const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        circle.setAttribute('r', '5');
        circle.setAttribute('class', `flow-particle ${theme}`);
        particlesGroup.appendChild(circle);
        
        // Highlight connection line
        pathEl.classList.add('active', theme === 'error' ? 'red' : 'green');
        
        // Node updates along the way
        pulseNode(currentSegment, theme === 'error' ? 'red' : 'cyan');

        const duration = 400; // ms per segment
        const start = performance.now();
        const samples = PATH_CACHE[pathId];
        
        function step(now) {
            const progress = Math.min((now - start) / duration, 1);
            let point;
            if (samples) {
                const index = Math.min(Math.floor(progress * 100), 100);
                point = samples[index];
            } else {
                const totalLength = pathEl.getTotalLength();
                const pt = pathEl.getPointAtLength(progress * totalLength);
                point = { x: pt.x, y: pt.y };
            }
            
            circle.setAttribute('cx', point.cx || point.x);
            circle.setAttribute('cy', point.cy || point.y);
            
            if (progress < 1) {
                requestAnimationFrame(step);
            } else {
                circle.remove();
                pathEl.classList.remove('active', 'red', 'green');
                
                // Final node pulse on completion of segment
                if (currentSegment === segments.length - 1) {
                    pulseNode(6, theme === 'error' ? 'red' : 'yellow');
                    activeParticles--;
                }
                
                currentSegment++;
                runNextSegment();
            }
        }
        
        requestAnimationFrame(step);
    }
    
    runNextSegment();
}

// Metrics Calculation
setInterval(() => {
    const elapsed = (Date.now() - lastCountReset) / 1000;
    const tput = (rollingEvents / elapsed).toFixed(1);
    
    throughputValue.innerHTML = `${tput} <span class="unit">evt/s</span>`;
    
    // Update success rate
    if (eventCount > 0) {
        const rate = Math.round(((eventCount - dlqCount) / eventCount) * 100);
        successValue.innerHTML = `${rate}<span class="unit">%</span>`;
    }
    
    rollingEvents = 0;
    lastCountReset = Date.now();
}, 2000);

// Simulator Integration
async function injectEvent(payload, isRaw = false, format = 'json_app') {
    let url = '/v1/logs';
    let body = JSON.stringify(payload);
    let contentType = 'application/json';
    
    if (isRaw) {
        url = `/v1/logs/raw?source_format=${format}`;
        body = payload;
        contentType = 'text/plain';
    }
    
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': contentType },
            body: body
        });
        if (!response.ok) {
            const data = await response.json();
            appendSystemLog(`[ERROR] Injection failed (${response.status}): ${data.detail || 'unknown error'}`);
        }
    } catch (e) {
        appendSystemLog(`[ERROR] Network error during injection: ${e}`);
    }
}

// Button Handlers
simHealthyBtn.addEventListener('click', () => {
    const healthyLog = {
        service: 'checkout-api',
        level: 'INFO',
        message: 'checkout completed successfully for transaction ' + Math.random().toString(36).substring(7).toUpperCase(),
        status_code: 200,
        path: '/checkout/pay',
        http_method: 'POST',
        response_time_ms: Math.floor(Math.random() * 150) + 50
    };
    injectEvent(healthyLog);
});

simErrorBtn.addEventListener('click', () => {
    const errorLog = {
        service: 'payments-api',
        level: 'ERROR',
        message: 'database connection pool exhausted on cluster-03: connection timeout',
        status_code: 500,
        path: '/v2/charge',
        http_method: 'POST',
        response_time_ms: Math.floor(Math.random() * 1000) + 1500
    };
    injectEvent(errorLog);
});

simCorruptBtn.addEventListener('click', () => {
    // Send bad unparseable string to the raw log path to trigger DLQ
    const badRawLog = `127.0.0.1 - - [17/Aug/2026:12:00:00 +0000] "MALFORMED REQUEST STRING" WRONG_STATUS_CODE 0`;
    injectEvent(badRawLog, true, 'nginx_combined');
});

// Clear log button
clearTerminalBtn.addEventListener('click', () => {
    terminal.innerHTML = '';
    appendSystemLog('Log terminal cleared.');
});

// Start connection
precalculatePaths();
connect();
