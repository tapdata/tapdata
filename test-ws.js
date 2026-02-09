
const http = require('http');

const options = {
  hostname: 'tm',
  port: 3030,
  path: '/ws/agent?id=decc4467-83e8-4dae-b5d7-1fe24d9ae6bc&access_token=a1b7cc341cc743afb3e45906fc45bc0489e29fde6f474233b1612562ce036b34',
  method: 'GET',
  headers: {
    'Connection': 'Upgrade',
    'Upgrade': 'websocket',
    'Origin': 'http://localhost:8080',
    'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
    'Sec-WebSocket-Version': '13'
  }
};

const req = http.request(options);
req.end();

req.on('upgrade', (res, socket, upgradeHead) => {
  console.log('got upgraded!');
  console.log('Response Headers:', res.headers);
  socket.end();
  process.exit(0);
});

req.on('response', (res) => {
  console.log('STATUS: ' + res.statusCode);
  console.log('HEADERS: ' + JSON.stringify(res.headers));
  res.on('data', chunk => console.log('BODY: ' + chunk));
  // process.exit(1); 
});

req.on('error', (e) => {
  console.error(e);
  process.exit(1);
});
