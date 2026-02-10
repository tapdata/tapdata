const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const path = require('path');

const app = express();
const PORT = process.env.WEB_PORT || 8080;
const API_URL = process.env.TM_API_URL || 'http://tm:3030';

console.log(`Starting server with API_URL: ${API_URL}`);

// Proxy configuration
const proxyOptions = {
    target: API_URL,
    changeOrigin: true,
    ws: true,
    logLevel: 'debug',
    logger: console,
    proxyTimeout: 60000,
    timeout: 60000,
    onProxyReq: (proxyReq, req, res) => {
        console.log(`[${new Date().toISOString()}] Proxying ${req.method} request to: ${API_URL}${req.url}`);
    },
    onError: (err, req, res) => {
        console.error(`[${new Date().toISOString()}] Proxy error:`, err);
        if (res && res.status) {
             res.status(500).send('Proxy error');
        }
    }
};

const wsProxy = createProxyMiddleware({
    target: API_URL,
    ws: true,
    xfwd: true, // Add X-Forwarded headers
    logLevel: 'debug',
    logger: console,
    onProxyReqWs: (proxyReq, req, socket, options, head) => {
        console.log(`Proxying WebSocket request to: ${API_URL}${req.url}`);
    },
    onError: (err, req, socket) => {
        console.error('WebSocket Proxy error:', err);
        if (socket && socket.destroy) {
             socket.destroy();
        }
    }
});

console.log('wsProxy keys:', Object.keys(wsProxy));

app.use('/api', createProxyMiddleware({
    ...proxyOptions,
    pathRewrite: {
        '^/': '/api/'
    }
}));

app.use('/ws', createProxyMiddleware({
    ...proxyOptions,
    pathRewrite: {
        '^/': '/ws/'
    }
}));

app.use('/oauth2', createProxyMiddleware({
    ...proxyOptions,
    pathRewrite: {
        '^/': '/oauth2/'
    }
}));

// Serve static files
app.use(express.static(path.join(__dirname, 'dist')));

// SPA fallback
app.use((req, res) => {
    res.sendFile(path.join(__dirname, 'dist', 'index.html'));
});

const server = app.listen(PORT, () => {
    console.log(`Web server running on port ${PORT}`);
});

// Handle WebSocket upgrades
server.on('upgrade', (req, socket, head) => {
    console.log('Upgrade request for:', req.url);
    if (req.url.startsWith('/ws')) {
        console.log('Upgrading to WebSocket:', req.url);
        try {
            if (typeof wsProxy.upgrade === 'function') {
                wsProxy.upgrade(req, socket, head);
            } else {
                console.error('wsProxy.upgrade is not a function!');
                socket.destroy();
            }
        } catch (err) {
            console.error('Error during wsProxy.upgrade:', err);
            socket.destroy();
        }
    } else {
        console.log('Destroying socket for non-ws upgrade:', req.url);
        socket.destroy();
    }
});
